SELECT
    id,
    job_title,
    hire_date,
    salary,
    SUM(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees
