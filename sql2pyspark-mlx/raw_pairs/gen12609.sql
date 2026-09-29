SELECT
    location,
    CASE WHEN budget >= 3822000 THEN 'high' WHEN budget >= 819000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    COUNT(DISTINCT budget) AS unique_locations,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE location IN ('Singapore', 'London', 'Bangkok') AND dept_name IN ('Legal', 'Engineering', 'Marketing') AND dept_name = 'Marketing'
GROUP BY location, budget_bucket
HAVING COUNT(DISTINCT budget) >= 4;
