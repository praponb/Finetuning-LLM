SELECT
    location,
    CASE WHEN budget >= 3138000 THEN 'high' WHEN budget >= 866000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values,
    MIN(budget) AS min_budget,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
FROM departments
WHERE dept_name IS NULL AND location = 'Tokyo'
GROUP BY location, CASE WHEN budget >= 3138000 THEN 'high' WHEN budget >= 866000 THEN 'mid' ELSE 'low' END, dept_name
HAVING MIN(budget) >= 3741000;
