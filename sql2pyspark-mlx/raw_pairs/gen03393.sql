SELECT
    id,
    level,
    dept_id,
    hire_date,
    salary,
    ROW_NUMBER() OVER (PARTITION BY level, dept_id ORDER BY hire_date, id) AS hire_date_row,
    SUM(salary) OVER (PARTITION BY level ORDER BY hire_date, id) AS running_salary
FROM employees;
