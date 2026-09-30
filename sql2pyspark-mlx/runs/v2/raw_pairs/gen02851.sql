SELECT * FROM (SELECT id, dept_id, hire_date, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS pos FROM employees) AS t WHERE pos <= 3 ORDER BY hire_date
