SELECT
    id,
    dept_id,
    hire_date,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS row_num
FROM employees;
