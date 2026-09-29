WITH top_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) row_num
    FROM employees
)
SELECT
    id,
    job_title,
    salary,
    manager_id
FROM top_employees
WHERE row_num <= 2
ORDER BY job_title
