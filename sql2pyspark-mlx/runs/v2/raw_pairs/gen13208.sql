SELECT id, salary, DATE_SUB(hire_date, 33) AS hire_date_minus_33d, TRIM(level) AS clean_level FROM employees WHERE manager_id IS NOT NULL AND YEAR(hire_date) >= 2025
