SELECT id, level, hire_date, full_name FROM (SELECT id, level, hire_date, full_name, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS rnk FROM employees) AS t WHERE rnk <= 3;
