SELECT
    CASE WHEN budget >= 4218000 THEN 'high' WHEN budget >= 1626000 THEN 'mid' ELSE 'low' END budget_bucket,
    location,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) location_values,
    COUNT(*) row_count,
    ROUND(STDDEV(budget), 2) stddev_budget
FROM departments
WHERE budget <= 2030000
GROUP BY CASE WHEN budget >= 4218000 THEN 'high' WHEN budget >= 1626000 THEN 'mid' ELSE 'low' END, location
