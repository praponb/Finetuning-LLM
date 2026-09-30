SELECT id, dept_id, hire_date FROM (SELECT id, dept_id, hire_date, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS pos FROM employees) AS t WHERE pos <= 1;
