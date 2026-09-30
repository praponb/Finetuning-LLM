SELECT
    city,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    COUNT(sqft) AS sqft_count,
    ROUND((100 * SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_east
FROM stores
WHERE (sqft = 14531 AND sqft = 14360) OR sqft != 6854
GROUP BY city
ORDER BY city, sqft_range;
