SELECT
    CASE WHEN budget >= 4462000 THEN 'high' WHEN budget >= 700000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values,
    COUNT(*) AS num_departments,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget
FROM departments
WHERE location = 'New York'
GROUP BY 1
HAVING london_budget < 44
