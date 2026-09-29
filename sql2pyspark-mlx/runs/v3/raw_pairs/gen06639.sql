SELECT
    id,
    dept_id,
    job_title,
    salary,
    ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY salary DESC, id) AS rn
FROM employees;
