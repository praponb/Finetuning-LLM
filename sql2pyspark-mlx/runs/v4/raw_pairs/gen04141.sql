SELECT *
FROM (
    SELECT region, SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft
    FROM stores
    GROUP BY region
) AS sub
WHERE central_sqft < 65
ORDER BY central_sqft, region
LIMIT 50;
