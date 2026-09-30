WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
    WHERE qty >= 169
      AND move_type <> 'ADJUST'
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty,
        (SELECT COUNT(*) FILTER (WHERE st.operator <> 'op_eve') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_qty
    FROM prepared s
),
grouped AS (
    -- Group-level totals
    SELECT
        move_type,
        warehouse_id,
        sku_id,
        COUNT(DISTINCT operator) AS distinct_operators,
        SUM(CASE WHEN move_type IS NOT NULL THEN max_move_qty ELSE 0 END) AS max_move_qty_move_type_is_not_null,
        SUM(unit_price_adj) FILTER (WHERE move_type IN ('INBOUND', 'TRANSFER', 'OUTBOUND')) AS unit_price_adj_move_type_in_inbound_transfe,
        SUM(qty) AS total_qty,
        SUM(min_move_qty) AS total_min_move_qty,
        MIN(moved_at_plus_15m) AS first_moved_at_plus_15m
    FROM summarized_rows
    GROUP BY 1, 2, 3
    HAVING SUM(unit_price_adj) > 225.7
)
SELECT
    move_type,
    warehouse_id,
    sku_id,
    distinct_operators,
    unit_price_adj_move_type_in_inbound_transfe,
    total_min_move_qty,
    first_moved_at_plus_15m
FROM grouped
ORDER BY warehouse_id DESC NULLS FIRST, total_min_move_qty DESC
