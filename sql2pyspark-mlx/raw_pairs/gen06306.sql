SELECT
    id,
    job_title,
    manager_id,
    hire_date,
    salary,
    SUM(salary) OVER (PARTITION BY job_title, manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS rn
FROM employees
WHERE salary >= 103000
