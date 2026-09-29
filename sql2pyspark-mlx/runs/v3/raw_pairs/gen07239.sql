SELECT
    id,
    manager_id,
    salary,
    job_title
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) pos
    FROM employees
) t
WHERE pos <= 3
ORDER BY salary DESC
