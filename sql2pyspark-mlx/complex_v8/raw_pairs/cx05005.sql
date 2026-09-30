WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        ABS(unit_price - 209.4) AS unit_price_distance,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE operator = 'op_bee'
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(st.qty), 2) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS avg_move_qty
    FROM prepared_moves s
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND MONTH(st.moved_at) IN (1, 2)
              AND st.unit_price < 330.0
        )
      AND s.qty_per_unit_price >= 1.0
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price,
    unit_price_distance,
    moved_at_date,
    avg_move_qty,
    CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group
FROM screened
ORDER BY unit_price, move_type DESC
