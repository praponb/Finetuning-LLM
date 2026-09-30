SELECT id, dept_id, salary, NTILE(4) OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS bucket FROM employees
