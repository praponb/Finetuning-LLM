SELECT
    id,
    full_name,
    email,
    job_title,
    salary,
    hire_date,
    is_remote,
    level
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY hire_date DESC NULLS LAST, id) AS dedup_rank
    FROM employees
) t
WHERE dedup_rank = 1;
