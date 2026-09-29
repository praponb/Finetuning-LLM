WITH base AS (
    SELECT *, TRIM(job_title) AS clean_job_title
    FROM employees
    WHERE dept_id IN (5, 23, 31)
),
level_summary AS (
    SELECT
        level,
        is_remote,
        SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary,
        MIN(salary) AS min_salary
    FROM base
    GROUP BY level, is_remote
)
SELECT
    level,
    is_remote,
    l3_salary,
    min_salary
FROM level_summary
WHERE min_salary > 118000
ORDER BY level DESC;
