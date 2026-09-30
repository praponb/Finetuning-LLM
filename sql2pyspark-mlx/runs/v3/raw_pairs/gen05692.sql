WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE sqft = 15451
),
stores_stats AS (
    SELECT city, ROUND(STDDEV(sqft), 2) stddev_sqft, SUM(sqft) total_sqft
    FROM valid_stores
    GROUP BY city
)
SELECT city, stddev_sqft, total_sqft
FROM stores_stats
WHERE total_sqft >= 52000
ORDER BY city
LIMIT 10;
