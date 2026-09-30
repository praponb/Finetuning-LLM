SELECT id, level, hire_date, DENSE_RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) hire_date_dense_rank FROM employees WHERE level = 'L3'
