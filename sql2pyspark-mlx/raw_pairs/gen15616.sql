WITH filtered_departments AS (
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE dept_name IN ('Sales', 'HR', 'Finance')
),
summary AS (
    SELECT CASE WHEN budget >= 3957000 THEN 'high' WHEN budget >= 1342000 THEN 'mid' ELSE 'low' END AS budget_bucket, SORT_ARRAY(COLLECT_SET(location)) AS location_list
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 3957000 THEN 'high' WHEN budget >= 1342000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, location_list
FROM summary
ORDER BY budget_bucket DESC;
