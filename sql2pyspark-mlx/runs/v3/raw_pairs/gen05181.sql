SELECT id, dept_name, LOWER(location) location_lower FROM departments WHERE dept_name IN ('HR', 'Engineering') AND location IN ('Bangkok', 'Tokyo');
