WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE warehouse_id <= 4
      AND unit_price <= 330.0
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
)
SELECT
    warehouse_id,
    move_type,
    ROUND(SUM(unit_price) / NULLIF(SUM(qty), 0), 3) AS unit_price_to_qty_ratio,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
    SUM(qty_filled) FILTER (WHERE operator = 'op_chan') AS qty_filled_operator_op_chan
FROM screened
GROUP BY CUBE(warehouse_id, move_type)
ORDER BY warehouse_id NULLS LAST, move_type NULLS LAST
