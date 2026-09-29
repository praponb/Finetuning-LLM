SELECT
    CASE WHEN budget >= 3481000 THEN 'high' WHEN budget >= 1837000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    dept_name,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
FROM departments
GROUP BY CASE WHEN budget >= 3481000 THEN 'high' WHEN budget >= 1837000 THEN 'mid' ELSE 'low' END, location, dept_name;
