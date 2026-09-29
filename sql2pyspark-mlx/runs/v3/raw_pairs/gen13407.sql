SELECT
    id,
    level,
    hire_date,
    dept_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS hire_date_row,
    MIN(salary) OVER (PARTITION BY dept_id) AS dept_id_min_salary
FROM employees
