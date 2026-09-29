SELECT id, hire_date, level, ROW_NUMBER() OVER (PARTITION BY MONTH(hire_date), level ORDER BY hire_date DESC, id) hire_date_row FROM employees ORDER BY id DESC
