WITH valid_stores AS (
    SELECT *, CONCAT_WS('-', city, store_name) city_store_name_key
    FROM stores
    WHERE opened_date >= '2024-04-21' AND (store_name IS NULL OR sqft = 7568)
),
agg AS (
    SELECT
        city,
        region,
        COUNT(sqft) sqft_count,
        ROUND((100 * SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_west
    FROM valid_stores
    GROUP BY city, region
)
SELECT *
FROM agg
WHERE sqft_count < 5
ORDER BY pct_west, city, region
LIMIT 10;
