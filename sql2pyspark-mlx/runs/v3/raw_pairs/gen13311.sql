SELECT
    id,
    job_title,
    hire_date,
    dept_id,
    salary,
    DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date) AS hire_date_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn
FROM employees
WHERE job_title IN ('Director', 'Intern')
