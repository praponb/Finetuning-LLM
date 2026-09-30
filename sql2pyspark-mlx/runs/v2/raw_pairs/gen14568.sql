SELECT
    id,
    manager_id,
    hire_date,
    level,
    salary,
    ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS row_num,
    salary - LAG(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS salary_change
FROM employees
WHERE dept_id = 28
