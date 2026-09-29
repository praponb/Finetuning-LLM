WITH base AS (
    SELECT *, INITCAP(TRIM(full_name)) AS full_name_title
    FROM employees
    WHERE manager_id IS NULL
),
employees_stats AS (
    SELECT job_title, is_remote, MIN(salary) AS min_salary
    FROM base
    GROUP BY job_title, is_remote
)
SELECT job_title, is_remote, min_salary
FROM employees_stats
ORDER BY 1;
