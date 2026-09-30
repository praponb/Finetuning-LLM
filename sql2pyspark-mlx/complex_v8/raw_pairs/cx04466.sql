WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE (TO_DATE(moved_at) < DATE '2025-02-18' OR sku_id IS NOT NULL)
)
SELECT operator, qty_minus_unit_price, 'move' AS record_type
FROM valid
WHERE warehouse_id <= 4
  AND HOUR(moved_at) BETWEEN 9 AND 14
UNION ALL
SELECT move_type, unit_price, 'move_2' AS record_type
FROM stock_moves
