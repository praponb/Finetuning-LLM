SELECT * FROM (SELECT id, dept_id, hire_date, full_name, RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) AS pos FROM employees) AS ranked WHERE pos = 1 ORDER BY hire_date DESC, full_name ASC
