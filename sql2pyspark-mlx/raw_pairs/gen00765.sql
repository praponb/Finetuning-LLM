WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CONCAT_WS('-', region, store_name) AS region_store_name_key,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE opened_date > DATE '2023-07-06'
      AND region IS NOT NULL
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    region_store_name_key,
    region_filled,
    AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft,
    SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft,
    MIN(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft
FROM filtered
WHERE city = 'New York';
