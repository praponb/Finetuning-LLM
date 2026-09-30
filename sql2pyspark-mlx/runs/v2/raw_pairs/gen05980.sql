SELECT
    id,
    dept_id,
    salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 3) salary_share
FROM employees
