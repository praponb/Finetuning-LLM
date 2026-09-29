WITH base AS (
    SELECT *
    FROM stores
    WHERE region = 'Central' AND LOWER(store_name) LIKE '%all%' AND region = 'Central'
),
region_summary AS (
    SELECT region, AVG(sqft) AS avg_sqft, SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END) AS london_count
    FROM base
    GROUP BY region
)
SELECT *
FROM region_summary
WHERE london_count > 73
ORDER BY london_count
