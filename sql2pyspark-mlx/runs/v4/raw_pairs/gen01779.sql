SELECT
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_dense_rank,
    NTILE(3) OVER (PARTITION BY dept_id ORDER BY salary, id) AS salary_bucket,
    SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees;
