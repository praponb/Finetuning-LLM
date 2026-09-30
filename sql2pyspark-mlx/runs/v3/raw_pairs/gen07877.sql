SELECT
    id,
    dept_id,
    hire_date,
    salary,
    level,
    salary - LAG(salary, 2) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS salary_change,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS rn
FROM employees
WHERE is_remote;
