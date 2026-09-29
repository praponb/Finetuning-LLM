WITH Base AS (
    SELECT *
    FROM employees
    WHERE full_name IS NOT NULL AND (job_title <> 'Manager' OR level IN ('L5', 'L1', 'L4'))
),
employees_stats AS (
    SELECT level, ROUND(STDDEV(salary), 2) AS stddev_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM Base
    GROUP BY level
)
SELECT level, stddev_salary, level_values
FROM employees_stats
WHERE stddev_salary < 79
