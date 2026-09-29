SELECT
    id,
    manager_id,
    job_title,
    hire_date,
    salary,
    SUM(salary) OVER (PARTITION BY manager_id, job_title ORDER BY hire_date, id) AS running_salary
FROM employees
ORDER BY job_title DESC, salary DESC;
