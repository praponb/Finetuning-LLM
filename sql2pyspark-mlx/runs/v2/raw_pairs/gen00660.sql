SELECT id, hire_date, ROW_NUMBER() OVER (ORDER BY hire_date, id) AS row_num
FROM employees
ORDER BY id DESC, hire_date;
