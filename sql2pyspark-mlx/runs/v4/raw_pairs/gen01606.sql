WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(budget) OVER (PARTITION BY location) AS location_count_budget,
        LAG(budget, 2, 0) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS prev_budget
    FROM departments
)
SELECT
    id,
    dept_name,
    prev_budget,
    CASE WHEN dept_name RLIKE '^(Engineering|Legal)$' THEN 'PRIORITY' WHEN dept_name RLIKE '^(Sales|Marketing)$' THEN 'EXTENDED' ELSE 'CORE' END AS dept_name_zone
FROM with_windows
ORDER BY id DESC, dept_name_zone;
