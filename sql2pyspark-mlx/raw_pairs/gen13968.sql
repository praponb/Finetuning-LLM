SELECT id, hire_date, salary, dept_id, LAG(salary, 1) OVER (ORDER BY hire_date, id) AS prev_salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS hire_date_row FROM employees;
