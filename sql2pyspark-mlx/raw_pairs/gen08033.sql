WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        ROUND(stock_qty * 100, 2) AS stock_qty_scaled,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE brand IS DISTINCT FROM 'Initech'
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, brand ORDER BY stock_qty_scaled DESC NULLS LAST, id) AS rn
    FROM base
) x
WHERE rn = 1;
