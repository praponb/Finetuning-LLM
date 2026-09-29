SELECT id, dept_id, hire_date, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) AS hire_date_dense_rank FROM employees
