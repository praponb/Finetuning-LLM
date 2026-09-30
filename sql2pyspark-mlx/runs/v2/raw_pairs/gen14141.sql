WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE city IN ('New York', 'Singapore') AND region = 'West'
),
stores_stats AS (
    SELECT region, MAX(sqft) - MIN(sqft) AS sqft_range
    FROM filtered_stores
    GROUP BY region
)
SELECT *
FROM stores_stats
WHERE sqft_range < 21
ORDER BY 1 DESC;
