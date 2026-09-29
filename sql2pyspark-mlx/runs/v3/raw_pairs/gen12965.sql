SELECT *
FROM (
    SELECT
        id,
        job_title,
        salary,
        full_name,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) row_num
    FROM employees
) t
WHERE row_num = 1
