WITH ranked_employees AS (
    SELECT
        id,
        job_title,
        salary,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) rn
    FROM employees
)
SELECT id, job_title, salary
FROM ranked_employees
WHERE rn <= 3
ORDER BY id
