WITH recent_stores AS (
    SELECT *, ROUND(sqft * 1.1, 2) AS adjusted_sqft
    FROM stores
    WHERE region = 'East'
),
stores_stats AS (
    SELECT city, COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count, COUNT(*) AS total_count
    FROM recent_stores
    GROUP BY city
)
SELECT *
FROM stores_stats
WHERE total_count > 9
ORDER BY singapore_count DESC, city
LIMIT 20;
