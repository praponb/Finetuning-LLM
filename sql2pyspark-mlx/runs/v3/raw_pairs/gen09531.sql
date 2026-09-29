SELECT
    id,
    job_title,
    hire_date,
    salary,
    DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date) AS hire_date_dense_rank,
    SUM(salary) OVER (ORDER BY hire_date, id) AS running_salary
FROM employees
ORDER BY id DESC;
