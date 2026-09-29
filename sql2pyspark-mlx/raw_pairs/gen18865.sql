WITH valid AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        TRIM(full_name) AS clean_full_name
    FROM employees
    WHERE hire_date >= DATE '2023-05-31'
      AND level IS NOT NULL
)
SELECT
    manager_id,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    ROUND(AVG(salary), 2) AS avg_salary,
    COUNT_IF(job_title = 'Director') AS director_events,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles_2
FROM valid
GROUP BY manager_id
ORDER BY manager_id;
