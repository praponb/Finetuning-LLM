SELECT
    CASE WHEN budget >= 3120000 THEN 'high' WHEN budget >= 596000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    SUM(CASE WHEN location = 'Berlin' THEN budget ELSE 0 END) AS berlin_budget,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
FROM departments
WHERE location IS NOT NULL OR location IS NULL
GROUP BY budget_bucket, location
ORDER BY berlin_budget DESC
