WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        COALESCE(qty, 0) AS qty_filled,
        COALESCE(qty, 0) AS qty_filled_2
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND warehouse_id >= 2
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_qty
    FROM cleaned s
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        ROUND(qty_filled_2 / NULLIF(total_move_qty, 0), 3) AS qty_filled_2_per_total_move_qty,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label
    FROM with_stats
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    operator,
    unit_price,
    qty_filled,
    last_moved_at,
    move_count,
    total_move_qty,
    operator_or_unknown,
    qty_filled_2_per_total_move_qty,
    COALESCE(qty_filled_2, 0) AS qty_filled_2_filled,
    SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
FROM scored
ORDER BY move_type_prefix
