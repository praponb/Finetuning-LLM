SELECT
    id,
    job_title,
    salary,
    manager_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS rnk
    FROM employees
) AS ranked
WHERE rnk <= 2
