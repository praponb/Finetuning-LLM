WITH src AS (
    SELECT *, CASE WHEN location IN ('Singapore', 'Tokyo') THEN 'Core' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location IS NOT NULL
),
summary AS (
    SELECT CASE WHEN budget >= 3521000 THEN 'high' WHEN budget >= 687000 THEN 'mid' ELSE 'low' END AS budget_bucket, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
    FROM src
    GROUP BY CASE WHEN budget >= 3521000 THEN 'high' WHEN budget >= 687000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, dept_name_list
FROM summary
ORDER BY budget_bucket
LIMIT 5;
