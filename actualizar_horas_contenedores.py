import pandas as pd
import os
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union


class ContainerTimeUpdater:
    """
    Sistema optimizado para actualizar horas de salida de contenedores
    haciendo match entre archivo principal y logs de exportación.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa el actualizador con configuración personalizable.
        
        Args:
            config: Diccionario de configuración opcional
        """
        self.config = config or self._get_default_config()
        self.logger = self._setup_logging()
        self._cache = {}
    
    def _get_default_config(self) -> Dict:
        """Configuración por defecto del sistema"""
        return {
            'main_file': {
                'sheet_name': 'BBDD',
                'header_row_search_terms': ['Fecha', 'Contenedor', 'Placa', 'Hr salida'],
                'columns': {
                    'container': ['Contenedor', 'CONTENEDOR', 'Nro Contenedor'],
                    'plate': ['Placa 2', 'Placa', 'PLACA 2', 'PLACA'],  # Priorizar Placa 2
                    'time': ['Hr salida QP', 'HORA SALIDA QP', 'HORA_SALIDA_QP'],
                    'date': ['Fecha', 'FECHA', 'Date']
                }
            },
            'export_file': {
                'header_row_search_terms': ['NUMERO CONTENEDOR', 'PLACA DE CARRETA', 'HORA DE SALIDA'],
                'columns': {
                    'container': ['NUMERO CONTENEDOR', 'CONTENEDOR', 'CONTAINER'],
                    'plate': ['PLACA DE CARRETA', 'PLACA DEL TRACTO', 'PLACA', 'CARRETA'],
                    'time': ['HORA DE SALIDA', 'HORA SALIDA', 'HORA', 'SALIDA']
                }
            },
            'processing': {
                'max_header_search_rows': 20,
                'time_format': '%H:%M:%S',
                'encoding': 'utf-8',
                'date_formats': ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
            }
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Configura el sistema de logging"""
        logger = logging.getLogger('ContainerUpdater')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Handler para archivo
            file_handler = logging.FileHandler('container_update.log', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            
            # Handler para consola
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Formato
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def _load_excel_cached(self, file_path: str, sheet_name: str, header_row: Optional[int] = None) -> pd.DataFrame:
        """Carga archivos Excel con cache manual para evitar recargas"""
        cache_key = f"{file_path}_{sheet_name}_{header_row}"
        
        if cache_key in self._cache:
            self.logger.debug(f"Archivo cargado desde cache: {file_path}, hoja: {sheet_name}")
            return self._cache[cache_key].copy()
        
        try:
            df = pd.read_excel(
                file_path, 
                sheet_name=sheet_name, 
                header=header_row,
                engine='openpyxl'
            )
            self._cache[cache_key] = df.copy()
            self.logger.debug(f"Archivo cargado y guardado en cache: {file_path}, hoja: {sheet_name}")
            return df
        except Exception as e:
            self.logger.error(f"Error cargando {file_path}: {e}")
            raise
    
    def find_column_by_patterns(self, df: pd.DataFrame, patterns: List[str], column_type: str) -> Optional[str]:
        """
        Encuentra una columna basada en patrones de coincidencia.
        
        Args:
            df: DataFrame donde buscar
            patterns: Lista de patrones a buscar
            column_type: Tipo de columna para logging
            
        Returns:
            Nombre de la columna encontrada o None
        """
        df_columns_upper = [str(col).strip().upper() for col in df.columns]
        
        # Búsqueda exacta
        for pattern in patterns:
            pattern_upper = pattern.upper()
            if pattern_upper in df_columns_upper:
                idx = df_columns_upper.index(pattern_upper)
                found_col = df.columns[idx]
                self.logger.debug(f"Columna {column_type} encontrada (exacta): '{found_col}'")
                return found_col
        
        # Búsqueda parcial
        for pattern in patterns:
            pattern_upper = pattern.upper()
            for i, col_upper in enumerate(df_columns_upper):
                if pattern_upper in col_upper:
                    found_col = df.columns[i]
                    self.logger.debug(f"Columna {column_type} encontrada (parcial): '{found_col}' con patrón '{pattern}'")
                    return found_col
        
        self.logger.warning(f"No se encontró columna {column_type}. Patrones: {patterns}")
        return None
    
    def find_header_row(self, df: pd.DataFrame, required_patterns: List[str]) -> int:
        """
        Encuentra la fila que contiene los encabezados requeridos.
        
        Args:
            df: DataFrame donde buscar
            required_patterns: Patrones que deben estar presentes
            
        Returns:
            Índice de la fila de encabezados
        """
        max_rows = min(self.config['processing']['max_header_search_rows'], len(df))
        
        for i in range(max_rows):
            row_values = df.iloc[i].astype(str).str.upper().values
            matches = sum(1 for pattern in required_patterns 
                         if any(pattern.upper() in val for val in row_values if val != 'NAN'))
            
            if matches >= len(required_patterns) * 0.6:  # Al menos 60% de coincidencias
                self.logger.info(f"Encabezados encontrados en fila {i+1} ({matches}/{len(required_patterns)} coincidencias)")
                return i
        
        raise ValueError(f"No se encontraron encabezados con patrones: {required_patterns}")
    
    def normalize_text(self, text: Union[str, float, None]) -> str:
        """Normaliza texto para comparación"""
        if pd.isna(text):
            return ''
        text_str = str(text).strip().upper()
        # Eliminar espacios, guiones y caracteres especiales para contenedores/placas
        # Ejemplo: "MRKU 546694-7" -> "MRKU5466947"
        return re.sub(r'[^A-Z0-9]', '', text_str)
    
    def normalize_time(self, time_val: Union[str, datetime, None]) -> str:
        """
        Normaliza valores de tiempo a formato HH:MM:SS.
        
        Args:
            time_val: Valor de tiempo en cualquier formato
            
        Returns:
            Tiempo normalizado como string HH:MM:SS
        """
        if pd.isna(time_val):
            return ''
        
        try:
            # Si ya es datetime
            if isinstance(time_val, datetime):
                return time_val.strftime('%H:%M:%S')
            
            # Si es string, intentar parsearlo
            time_str = str(time_val).strip()
            
            # Patrones comunes de tiempo
            time_patterns = [
                r'(\d{1,2}):(\d{2}):(\d{2})',  # HH:MM:SS
                r'(\d{1,2}):(\d{2})',          # HH:MM
                r'(\d{1,2})\.(\d{2})\.(\d{2})', # HH.MM.SS
                r'(\d{1,2})\.(\d{2})',          # HH.MM
            ]
            
            for pattern in time_patterns:
                match = re.search(pattern, time_str)
                if match:
                    groups = match.groups()
                    hours = int(groups[0])
                    minutes = int(groups[1])
                    seconds = int(groups[2]) if len(groups) > 2 else 0
                    
                    # Validar rangos
                    if 0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds <= 59:
                        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            # Intentar convertir directamente con pandas
            time_obj = pd.to_datetime(time_str, errors='coerce')
            if pd.notna(time_obj):
                return time_obj.strftime('%H:%M:%S')
                
        except Exception as e:
            self.logger.debug(f"Error normalizando tiempo '{time_val}': {e}")
        
        return ''
    
    def load_and_prepare_main_file(self, file_path: str) -> pd.DataFrame:
        """
        Carga y prepara el archivo principal.
        
        Args:
            file_path: Ruta al archivo principal
            
        Returns:
            DataFrame preparado
        """
        self.logger.info(f"Cargando archivo principal: {file_path}")
        
        sheet_name = self.config['main_file']['sheet_name']
        
        # Los encabezados están en la fila 2 (índice 1)
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=1, engine='openpyxl')
        
        self.logger.info(f"Archivo cargado con {len(df)} registros y {len(df.columns)} columnas")
        
        # Mostrar algunos nombres de columnas para debugging
        print(f"\n=== PRIMERAS 15 COLUMNAS DEL ARCHIVO ===")
        for i, col in enumerate(df.columns[:15]):
            print(f"{i+1}. {col}")
        
        print(f"\n=== COLUMNAS ESPECÍFICAS QUE BUSCAMOS ===")
        target_names = ['Contenedor', 'Placa 2', 'Hr salida QP']
        for name in target_names:
            if name in df.columns:
                print(f"✅ '{name}' ENCONTRADA")
            else:
                print(f"❌ '{name}' NO ENCONTRADA")
        
        # Encontrar columnas requeridas usando nombres exactos
        column_mapping = {}
        
        # Buscar por nombres exactos primero
        if 'Contenedor' in df.columns:
            column_mapping['container'] = 'Contenedor'
        if 'Placa 2' in df.columns:
            column_mapping['plate'] = 'Placa 2'
        if 'Hr salida QP' in df.columns:
            column_mapping['time'] = 'Hr salida QP'
        if 'Fecha' in df.columns:
            column_mapping['date'] = 'Fecha'
        
        # Si no encontramos por nombres exactos, usar búsqueda por patrones
        if 'plate' not in column_mapping:
            for col_type, patterns in self.config['main_file']['columns'].items():
                if col_type not in column_mapping:
                    found_col = self.find_column_by_patterns(df, patterns, col_type)
                    if found_col:
                        column_mapping[col_type] = found_col
        
        # Verificar que se encontraron las columnas esenciales
        essential_cols = ['plate', 'time']  # Contenedor es opcional
        missing_cols = [col for col in essential_cols if col not in column_mapping]
        if missing_cols:
            print(f"\n❌ COLUMNAS FALTANTES: {missing_cols}")
            print("Columnas disponibles en el archivo:")
            for i, col in enumerate(df.columns):
                print(f"  {i+1}. '{col}'")
            raise ValueError(f"Columnas esenciales no encontradas: {missing_cols}")
        
        # NO renombrar columnas, solo crear referencias internas
        result_df = df.copy()
        
        # Guardar el mapeo para uso posterior
        result_df._column_mapping = column_mapping
        
        # Mostrar algunos datos de muestra del archivo principal
        print(f"\n=== MAPEO DE COLUMNAS DEL ARCHIVO PRINCIPAL ===")
        print(f"Contenedor -> {column_mapping.get('container', 'NO ENCONTRADO (OPCIONAL)')}")
        print(f"Placa -> {column_mapping.get('plate', 'NO ENCONTRADO')}")
        print(f"Hora -> {column_mapping.get('time', 'NO ENCONTRADO')}")
        
        print("\n=== MUESTRA DE DATOS DEL ARCHIVO PRINCIPAL ===")
        sample_indices = result_df.index[:10]
        for idx in sample_indices:
            container_val = result_df.at[idx, column_mapping.get('container', 'N/A')] if 'container' in column_mapping else 'SIN_CONTENEDOR'
            plate_val = result_df.at[idx, column_mapping['plate']] if 'plate' in column_mapping else ''
            time_val = result_df.at[idx, column_mapping['time']] if 'time' in column_mapping else ''
            
            container_norm = self.normalize_text(container_val) if container_val != 'SIN_CONTENEDOR' else ''
            plate_norm = self.normalize_text(plate_val)
            print(f"Fila {idx}: Contenedor='{container_val}' (norm: '{container_norm}'), Placa='{plate_val}' (norm: '{plate_norm}'), Hora='{time_val}'")

        self.logger.info(f"Archivo principal cargado: {len(result_df)} registros")
        return result_df
    
    def load_and_prepare_export_file(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        """
        Carga y prepara el archivo de exportación.
        
        Args:
            file_path: Ruta al archivo de exportación
            sheet_name: Nombre de la hoja a procesar
            
        Returns:
            DataFrame preparado
        """
        self.logger.info(f"Cargando archivo exportación: {file_path}, hoja: {sheet_name}")
        
        try:
            # Cargar sin encabezados para buscar la fila correcta
            df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='openpyxl')
            
            # Buscar fila de encabezados
            required_patterns = []
            for patterns in self.config['export_file']['columns'].values():
                required_patterns.extend(patterns[:2])
            
            header_row = self.find_header_row(df_raw, required_patterns)
            
            # Recargar con encabezados correctos
            df = self._load_excel_cached(file_path, sheet_name, header_row)
            
            # Encontrar columnas requeridas
            column_mapping = {}
            for col_type, patterns in self.config['export_file']['columns'].items():
                found_col = self.find_column_by_patterns(df, patterns, col_type)
                if found_col:
                    column_mapping[col_type] = found_col
            
            # Verificar columnas esenciales
            essential_cols = ['container', 'plate', 'time']
            missing_cols = [col for col in essential_cols if col not in column_mapping]
            if missing_cols:
                raise ValueError(f"Columnas esenciales no encontradas en exportación: {missing_cols}")
            
            # Crear DataFrame limpio
            result_df = pd.DataFrame()
            for col_type, col_name in column_mapping.items():
                result_df[col_type] = df[col_name]
            
            # Limpiar datos
            result_df = result_df.dropna(subset=['container', 'plate', 'time'])
            result_df = result_df[
                (result_df['container'].astype(str).str.strip() != '') &
                (result_df['plate'].astype(str).str.strip() != '') &
                (result_df['time'].astype(str).str.strip() != '')
            ]
            
            self.logger.info(f"Archivo exportación cargado: {len(result_df)} registros válidos")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error cargando archivo exportación {file_path}, hoja {sheet_name}: {e}")
            raise
    
    def create_normalized_lookup(self, export_df: pd.DataFrame, export_date: datetime.date) -> Dict[Tuple[str, str], str]:
        """
        Crea diccionario de búsqueda desde el archivo de exportación.
        Solo incluye registros que tengan contenedor Y placa Y hora.
        
        Args:
            export_df: DataFrame del archivo de exportación
            export_date: Fecha específica de esta hoja (para match exacto)
            
        Returns:
            Diccionario con claves (container_norm, plate_norm) y valores time_norm
        """
        lookup = {}
        skipped_no_container = 0
        skipped_no_plate = 0
        skipped_no_time = 0
        
        print(f"\n=== PROCESANDO ARCHIVO DE EXPORTACIÓN PARA FECHA: {export_date} ===")
        
        for _, row in export_df.iterrows():
            container_norm = self.normalize_text(row['container'])
            plate_norm = self.normalize_text(row['plate'])
            time_norm = self.normalize_time(row['time'])
            
            # REQUISITO ESTRICTO: Debe tener contenedor Y placa Y hora
            if not container_norm:
                skipped_no_container += 1
                continue
            if not plate_norm:
                skipped_no_plate += 1
                continue
            if not time_norm:
                skipped_no_time += 1
                continue
            
            # Solo agregar si tiene TODO
            key = (container_norm, plate_norm)
            lookup[key] = time_norm
            
            if len(lookup) <= 5:  # Log primeros 5 para debugging
                print(f"  Agregado: Contenedor='{container_norm}', Placa='{plate_norm}', Hora='{time_norm}'")
        
        self.logger.info(f"Diccionario creado para {export_date}: {len(lookup)} entradas válidas")
        self.logger.info(f"Omitidos: {skipped_no_container} sin contenedor, {skipped_no_plate} sin placa, {skipped_no_time} sin hora")
        
        return lookup
    
    def update_times_optimized(self, main_df: pd.DataFrame, lookup: Dict[Tuple[str, str], str], 
                             target_date: datetime.date) -> int:
        """
        Actualiza los tiempos en el DataFrame principal usando el diccionario de búsqueda.
        Solo actualiza registros que coincidan EXACTAMENTE en contenedor, placa Y fecha.
        
        Args:
            main_df: DataFrame principal a actualizar
            lookup: Diccionario de búsqueda con contenedor+placa
            target_date: Fecha específica a procesar
            
        Returns:
            Número de registros actualizados
        """
        updates = 0
        skipped_no_container = 0
        skipped_no_plate = 0
        skipped_wrong_date = 0
        skipped_no_match = 0
        
        # Obtener el mapeo de columnas original
        column_mapping = getattr(main_df, '_column_mapping', {})
        if not column_mapping:
            raise ValueError("No se encontró mapeo de columnas en el DataFrame")
        
        container_col = column_mapping.get('container', None)
        plate_col = column_mapping['plate'] 
        time_col = column_mapping['time']
        date_col = column_mapping.get('date', None)
        
        print(f"\n=== PROCESANDO ACTUALIZACIONES PARA FECHA: {target_date} ===")
        print(f"Usando columnas: Contenedor='{container_col}', Placa='{plate_col}', Hora='{time_col}', Fecha='{date_col}'")
        
        # Identificar registros que necesitan actualización
        needs_update_mask = (
            main_df[time_col].isna() | 
            (main_df[time_col].astype(str).str.strip() == '') |
            (main_df[time_col].astype(str).str.strip().str.upper().isin(['NAN', 'NONE', 'NULL']))
        )
        
        total_candidates = needs_update_mask.sum()
        self.logger.info(f"Registros candidatos para actualización: {total_candidates}")
        
        # Procesar cada registro que necesita actualización
        for idx in main_df[needs_update_mask].index:
            # 1. Verificar CONTENEDOR (obligatorio)
            container_val = main_df.at[idx, container_col] if container_col else ''
            container_norm = self.normalize_text(container_val) if container_col else ''
            
            if not container_norm:
                skipped_no_container += 1
                if skipped_no_container <= 3:
                    self.logger.debug(f"Fila {idx}: Omitido por contenedor vacío")
                continue
            
            # 2. Verificar PLACA (obligatorio)
            plate_val = main_df.at[idx, plate_col]
            plate_norm = self.normalize_text(plate_val)
            
            if not plate_norm:
                skipped_no_plate += 1
                if skipped_no_plate <= 3:
                    self.logger.debug(f"Fila {idx}: Omitido por placa vacía")
                continue
            
            # 3. Verificar FECHA (obligatorio - debe coincidir EXACTAMENTE)
            if date_col:
                record_date_val = main_df.at[idx, date_col]
                if pd.isna(record_date_val):
                    skipped_wrong_date += 1
                    continue
                    
                # Convertir fecha del registro a date
                if isinstance(record_date_val, datetime):
                    record_date = record_date_val.date()
                elif isinstance(record_date_val, pd.Timestamp):
                    record_date = record_date_val.date()
                else:
                    try:
                        record_date = pd.to_datetime(record_date_val).date()
                    except:
                        skipped_wrong_date += 1
                        continue
                
                # MATCH EXACTO DE FECHA
                if record_date != target_date:
                    skipped_wrong_date += 1
                    if skipped_wrong_date <= 3:
                        self.logger.debug(f"Fila {idx}: Fecha no coincide - Registro: {record_date}, Objetivo: {target_date}")
                    continue
            
            # 4. Buscar en el diccionario (contenedor + placa)
            key = (container_norm, plate_norm)
            if key in lookup:
                time_found = lookup[key]
                main_df.at[idx, time_col] = time_found
                updates += 1
                
                # Log de los primeros updates para debugging
                if updates <= 5:
                    self.logger.info(f"✅ ACTUALIZADO fila {idx}: Contenedor='{container_val}' -> '{container_norm}', Placa='{plate_val}' -> '{plate_norm}', Fecha={target_date}, Hora='{time_found}'")
            else:
                skipped_no_match += 1
                if skipped_no_match <= 3:
                    self.logger.debug(f"Fila {idx}: No encontrado match para Contenedor='{container_norm}', Placa='{plate_norm}', Fecha={target_date}")
        
        # Resumen final
        self.logger.info(f"RESUMEN DE ACTUALIZACIÓN PARA {target_date}:")
        self.logger.info(f"  ✅ Registros actualizados: {updates}")
        self.logger.info(f"  ⏭️  Omitidos sin contenedor: {skipped_no_container}")
        self.logger.info(f"  ⏭️  Omitidos sin placa: {skipped_no_plate}")
        self.logger.info(f"  📅 Omitidos por fecha incorrecta: {skipped_wrong_date}")
        self.logger.info(f"  ❌ Sin match encontrado: {skipped_no_match}")
        
        return updates
    
    def extract_date_from_export_filename(self, export_file_path: str, day_str: str) -> datetime.date:
        """
        Extrae la fecha exacta desde el nombre del archivo de exportación y el día de la hoja.
        
        Args:
            export_file_path: Ruta del archivo (ej: SALIDA_2025_05.xlsx)
            day_str: Día de la hoja (ej: "22")
            
        Returns:
            Fecha exacta como datetime.date
        """
        import re
        filename = os.path.basename(export_file_path)
        
        # Buscar patrón YYYY_MM en el nombre del archivo
        match = re.search(r'(\d{4})_(\d{2})', filename)
        if not match:
            raise ValueError(f"No se pudo extraer año y mes del archivo: {filename}")
        
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(day_str)
        
        try:
            target_date = datetime(year, month, day).date()
            self.logger.info(f"Fecha objetivo extraída: {target_date} (de {filename}, hoja {day_str})")
            return target_date
        except ValueError as e:
            raise ValueError(f"Fecha inválida: año={year}, mes={month}, día={day}. Error: {e}")

    def process_multiple_days(self, main_file_path: str, export_file_path: str, 
                            days_to_process: List[str]) -> Tuple[int, str]:
        """
        Procesa múltiples días y actualiza el archivo principal.
        REQUISITO: Match exacto por contenedor + placa + fecha.
        
        Args:
            main_file_path: Ruta al archivo principal
            export_file_path: Ruta al archivo de exportación
            days_to_process: Lista de días a procesar (formato DD)
            
        Returns:
            Tupla con (total_updates, output_path)
        """
        self.logger.info(f"Iniciando procesamiento de {len(days_to_process)} días: {days_to_process}")
        
        # Cargar archivo principal una sola vez
        main_df = self.load_and_prepare_main_file(main_file_path)
        total_updates = 0
        
        for day in days_to_process:
            try:
                self.logger.info(f"Procesando día: {day}")
                
                # Extraer fecha exacta del archivo y hoja
                target_date = self.extract_date_from_export_filename(export_file_path, day)
                
                # Cargar archivo de exportación para este día
                export_df = self.load_and_prepare_export_file(export_file_path, day)
                
                # Crear lookup y actualizar CON FECHA ESPECÍFICA
                lookup = self.create_normalized_lookup(export_df, target_date)
                day_updates = self.update_times_optimized(main_df, lookup, target_date)
                
                total_updates += day_updates
                self.logger.info(f"Día {day} completado: {day_updates} actualizaciones")
                
            except Exception as e:
                self.logger.error(f"Error procesando día {day}: {e}")
                continue
        
        # Guardar archivo actualizado
        output_path = self._save_updated_file(main_df, main_file_path)
        
        self.logger.info(f"Procesamiento completado. Total de actualizaciones: {total_updates}")
        return total_updates, output_path
    
    def _save_updated_file(self, df: pd.DataFrame, original_path: str) -> str:
        """
        Guarda el DataFrame actualizado en un nuevo archivo.
        
        Args:
            df: DataFrame a guardar
            original_path: Ruta del archivo original
            
        Returns:
            Ruta del archivo guardado
        """
        # Crear nombre de archivo único
        original_path_obj = Path(original_path)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = original_path_obj.parent / f"{original_path_obj.stem}_ACTUALIZADO_{timestamp}.xlsx"
        
        try:
            # Guardar el DataFrame actualizado manteniendo estructura original
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=self.config['main_file']['sheet_name'], index=False)
            
            self.logger.info(f"Archivo guardado: {output_path}")
            print(f"\n✅ Archivo guardado correctamente: {output_path}")
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Error guardando archivo: {e}")
            raise


def get_date_range_input() -> List[str]:
    """Solicita al usuario el rango de fechas a procesar"""
    print("\n=== Configuración de Fechas ===")
    print("Opciones:")
    print("1. Presione Enter para procesar solo el día anterior")
    print("2. Ingrese un día específico (formato DD, ej: 15)")
    print("3. Ingrese un rango (formato DD-DD, ej: 22-31)")
    
    date_input = input("\nSeleccione opción: ").strip()
    
    if not date_input:
        # Día anterior
        yesterday = datetime.now() - timedelta(days=1)
        return [yesterday.strftime('%d')]
    elif '-' in date_input:
        # Rango de días
        try:
            start_day, end_day = map(int, date_input.split('-'))
            return [f"{day:02d}" for day in range(start_day, end_day + 1)]
        except ValueError:
            print("Formato de rango inválido. Usando día anterior.")
            yesterday = datetime.now() - timedelta(days=1)
            return [yesterday.strftime('%d')]
    else:
        # Día específico
        try:
            day = int(date_input)
            return [f"{day:02d}"]
        except ValueError:
            print("Formato de día inválido. Usando día anterior.")
            yesterday = datetime.now() - timedelta(days=1)
            return [yesterday.strftime('%d')]


def main():
    """Función principal del programa"""
    print("=" * 60)
    print("    SISTEMA DE ACTUALIZACIÓN DE HORAS DE SALIDA")
    print("    Versión 2.0 - Optimizada y Refactorizada")
    print("=" * 60)
    
    try:
        # Solicitar rutas de archivos
        print("\n=== Configuración de Archivos ===")
        main_file_path = input("Ruta del archivo PRINCIPAL (Lead time OQ1): ").strip('"')
        export_file_path = input("Ruta del archivo EXPORTACIÓN (SALIDA_YYYY_MM): ").strip('"')
        
        # Validar archivos
        if not os.path.exists(main_file_path):
            print(f"❌ ERROR: Archivo principal no encontrado: {main_file_path}")
            return
        
        if not os.path.exists(export_file_path):
            print(f"❌ ERROR: Archivo de exportación no encontrado: {export_file_path}")
            return
        
        # Obtener fechas a procesar
        days_to_process = get_date_range_input()
        print(f"\nDías a procesar: {', '.join(days_to_process)}")
        
        # Crear updater y procesar
        updater = ContainerTimeUpdater()
        
        print("\n" + "=" * 60)
        print("INICIANDO PROCESAMIENTO...")
        print("=" * 60)
        
        total_updates, output_path = updater.process_multiple_days(
            main_file_path, export_file_path, days_to_process
        )
        
        # Mostrar resultados
        print("\n" + "=" * 60)
        print("PROCESAMIENTO COMPLETADO")
        print("=" * 60)
        print(f"✅ Total de registros actualizados: {total_updates}")
        print(f"📁 Archivo guardado en: {output_path}")
        
        if total_updates == 0:
            print("\n⚠️  No se realizaron actualizaciones.")
            print("Posibles causas:")
            print("- Los datos ya están actualizados")
            print("- No hay coincidencias entre los archivos")
            print("- Los formatos de datos no coinciden")
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        logging.error(f"Error en main: {e}", exc_info=True)
    
    finally:
        input("\nPresione Enter para salir...")


if __name__ == "__main__":
    main()