SELECT
    dept_id,
    job_title,
    manager_id,
    CAST(is_remote AS INT) AS is_remote_flag
FROM employees
WHERE LENGTH(full_name) > 10 AND (hire_date BETWEEN '2023-05-01' AND '2024-07-01' OR manager_id IS NOT NULL)
ORDER BY 3 DESC, job_title
