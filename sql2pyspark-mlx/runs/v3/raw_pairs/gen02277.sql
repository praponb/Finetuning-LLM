SELECT
    region,
    ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count
FROM stores
WHERE city IN ('Singapore', 'Bangkok', 'Tokyo')
GROUP BY region;
