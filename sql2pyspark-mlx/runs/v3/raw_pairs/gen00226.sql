SELECT
    city,
    region,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) AS central_count
FROM stores
GROUP BY city, region
HAVING MAX(sqft) - MIN(sqft) < 42;
