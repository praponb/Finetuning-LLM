SELECT id, salary, ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS row_num FROM employees
