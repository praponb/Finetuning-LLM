SELECT city, east_sqft, avg_sqft
FROM (
    SELECT city, SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft, AVG(sqft) AS avg_sqft
    FROM stores
    WHERE city IN ('Chiang Mai', 'Berlin')
    GROUP BY city
) AS agg
WHERE east_sqft > 77
ORDER BY east_sqft DESC
