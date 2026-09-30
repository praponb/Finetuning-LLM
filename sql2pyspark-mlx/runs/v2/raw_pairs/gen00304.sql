WITH ValidStores AS (
    SELECT *
    FROM stores
    WHERE MONTH(opened_date) IN (7, 8, 12)
),
stores_stats AS (
    SELECT
        region,
        QUARTER(opened_date) AS opened_quarter,
        MAX(sqft) AS max_sqft,
        COUNT(DISTINCT sqft) AS unique_regions
    FROM ValidStores
    GROUP BY region, QUARTER(opened_date)
)
SELECT
    region,
    opened_quarter,
    max_sqft,
    unique_regions
FROM stores_stats
ORDER BY region, max_sqft
