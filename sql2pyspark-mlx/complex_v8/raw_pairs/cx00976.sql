WITH cleaned AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, w.wh_code AS warehouse_wh_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at, sku_id, move_type, unit_price
    FROM with_details
) src
PIVOT (
    MIN(unit_price)
    FOR move_type IN ('INBOUND', 'ADJUST', 'TRANSFER', 'OUTBOUND')
)
ORDER BY moved_at, sku_id
