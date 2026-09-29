SELECT id, hire_date, dept_id, salary, ROW_NUMBER() OVER (ORDER BY hire_date, id) AS row_num, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk FROM employees;
