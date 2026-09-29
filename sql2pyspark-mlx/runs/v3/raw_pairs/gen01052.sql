SELECT
    CASE WHEN city IN ('Berlin', 'New York') THEN 'tier_1' ELSE 'other' END AS city_group,
    SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count,
    ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
    COUNT(DISTINCT sqft) AS unique_citys
FROM stores
GROUP BY 1
ORDER BY unique_citys DESC
