SELECT id, level, DATE_TRUNC('year', hire_date) AS hire_date_period FROM employees WHERE manager_id IN (6, 11, 32) ORDER BY id DESC;
