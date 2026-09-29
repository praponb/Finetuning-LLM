WITH base AS (
    SELECT *
    FROM stores
    WHERE sqft BETWEEN 8685 AND 13382
),
region_summary AS (
    SELECT
        region,
        city,
        COUNT(DISTINCT sqft) AS unique_citys,
        COUNT(*) AS row_count
    FROM base
    GROUP BY region, city
)
SELECT region, city, row_count
FROM region_summary
ORDER BY row_count DESC
