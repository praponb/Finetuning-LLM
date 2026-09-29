WITH top_employees AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) pos FROM employees) SELECT * FROM top_employees WHERE pos <= 2;
