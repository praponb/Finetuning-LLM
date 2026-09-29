WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
)
-- Final projection
SELECT
    id,
    unit_price,
    unit_price_bucket,
    ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
FROM filtered_moves
WHERE sku_id IS NOT NULL
ORDER BY unit_price_bucket, unit_price_per_qty
