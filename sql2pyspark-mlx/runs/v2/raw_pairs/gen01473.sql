WITH src AS (
    SELECT *, DATE_TRUNC('month', hire_date) AS hire_date_period
    FROM employees
    WHERE is_remote
),
employees_stats AS (
    SELECT
        YEAR(hire_date) AS hire_year,
        DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
        COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
    FROM src
    GROUP BY YEAR(hire_date), DATE_FORMAT(hire_date, 'yyyy-MM')
)
SELECT
    hire_year,
    hire_month,
    level_values,
    l2_count
FROM employees_stats
WHERE l2_count > 8
ORDER BY level_values DESC, hire_year, hire_month
LIMIT 1;
