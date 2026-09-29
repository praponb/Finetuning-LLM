WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(price, 0) AS price_filled,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS NOT DISTINCT FROM 'Umbrella'
)
-- Multi-level aggregation
SELECT
    category,
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    APPROX_PERCENTILE(price, 0.9) AS price_p90,
    COUNT(*) AS row_count
FROM cleaned
GROUP BY GROUPING SETS ((category, DATE_TRUNC('QUARTER', launch_date)), (category))
ORDER BY quarter_start NULLS FIRST;
