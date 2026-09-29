WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        sku_id,
        move_type,
        ROUND(100 * COUNT(CASE WHEN move_type = 'INBOUND' THEN 1 END) / COUNT(*), 1) AS pct_move_type_inbound,
        COUNT(*) AS n_rows,
        MIN(qty) AS min_qty
    FROM cleaned
    GROUP BY warehouse_id, sku_id, move_type
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    warehouse_id,
    sku_id,
    move_type,
    pct_move_type_inbound,
    n_rows,
    min_qty,
    CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref,
    ROUND(pct_move_type_inbound * 1.07, 2) AS pct_move_type_inbound_adj
FROM summary
WHERE warehouse_id IS NOT NULL
ORDER BY pct_move_type_inbound_adj DESC, n_rows DESC
