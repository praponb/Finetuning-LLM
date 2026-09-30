WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    CASE WHEN unit_price >= 329.7 THEN 'A' WHEN unit_price >= 150.7 THEN 'B' ELSE 'C' END AS unit_price_tier,
    CASE WHEN qty >= 348 THEN 'HIGH' WHEN qty >= 152 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier
FROM scoped_moves
ORDER BY unit_price_tier DESC NULLS FIRST, id
LIMIT 10
