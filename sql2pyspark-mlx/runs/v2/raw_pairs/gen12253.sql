SELECT
    id,
    is_remote,
    hire_date,
    CAST(is_remote AS INT) AS is_remote_flag
FROM employees
WHERE job_title = 'Manager' AND dept_id IN (4, 6)
ORDER BY id DESC, hire_date
LIMIT 5
