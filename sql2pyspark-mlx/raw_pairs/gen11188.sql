WITH src AS (
    SELECT *
    FROM employees
    WHERE job_title = 'Director' AND job_title IS NOT NULL
),
employees_stats AS (
    SELECT job_title, SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END) AS engineer_count, SORT_ARRAY(COLLECT_SET(level)) AS level_list
    FROM src
    GROUP BY job_title
)
SELECT job_title, engineer_count, level_list
FROM employees_stats
ORDER BY job_title
LIMIT 5;
