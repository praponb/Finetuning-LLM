WITH filtered_employees AS (
    SELECT *, CONCAT_WS(',', level, full_name) AS level_full_name_key
    FROM employees
    WHERE NOT is_remote
),
summary AS (
    SELECT job_title, COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count, MAX(salary) - MIN(salary) AS salary_range
    FROM filtered_employees
    GROUP BY job_title
)
SELECT *
FROM summary
ORDER BY salary_range DESC
