WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        ABS(unit_price - 258.6) AS unit_price_distance
    FROM stock_moves
    WHERE qty > 137
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    unit_price_distance,
    SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
    ROUND(unit_price_distance * 1.5, 2) AS unit_price_distance_adj
FROM prepared_moves
ORDER BY sku_id DESC, move_type_prefix
