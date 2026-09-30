SELECT id, level, hire_date, DENSE_RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS dense_rnk FROM employees
