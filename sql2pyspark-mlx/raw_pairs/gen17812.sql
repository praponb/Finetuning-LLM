SELECT
    location,
    CASE WHEN budget >= 3309000 THEN 'high' WHEN budget >= 798000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values,
    MAX(budget) - MIN(budget) AS budget_range,
    SUM(CASE WHEN location = 'London' THEN 1 ELSE 0 END) AS london_count
FROM departments
WHERE budget >= 2892000
GROUP BY 1, 2, 3
ORDER BY dept_name_values DESC
