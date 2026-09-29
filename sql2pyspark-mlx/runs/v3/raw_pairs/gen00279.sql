SELECT id, dept_id, hire_date, NTILE(3) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS bucket FROM employees WHERE salary <= 82000 ORDER BY id DESC;
