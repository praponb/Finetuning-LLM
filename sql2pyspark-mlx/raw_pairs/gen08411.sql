SELECT id, dept_id, hire_date, RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) AS rnk FROM employees ORDER BY dept_id DESC, rnk DESC
