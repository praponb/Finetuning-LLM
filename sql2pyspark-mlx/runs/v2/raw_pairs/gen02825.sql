SELECT
    id,
    level,
    manager_id,
    salary,
    dept_id,
    hire_date,
    ROUND(salary / SUM(salary) OVER (PARTITION BY level, manager_id), 3) AS salary_share,
    SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees
WHERE email IS NULL
ORDER BY running_salary, 4;
