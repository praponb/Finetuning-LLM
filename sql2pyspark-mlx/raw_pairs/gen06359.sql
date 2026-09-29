WITH base AS (
    SELECT *
    FROM stores
    WHERE opened_date < '2024-01-01'
),
stores_stats AS (
    SELECT city, COUNT(DISTINCT sqft) AS unique_regions, SUM(sqft) AS total_sqft
    FROM base
    GROUP BY city
)
SELECT city, unique_regions, total_sqft
FROM stores_stats
WHERE unique_regions < 9
ORDER BY total_sqft DESC, unique_regions, city
LIMIT 50
