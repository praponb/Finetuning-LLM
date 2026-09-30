WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_bee'
      AND sku_id >= 3
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator IN ('op_eve', 'op_chan', 'op_ann')) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at
    FROM filtered_moves s
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        move_operator_count,
        operator,
        ROUND(STDDEV(move_operator_count), 2) AS stddev_move_operator_count,
        MIN(unit_price) AS min_unit_price,
        ROUND(100 * COUNT(CASE WHEN move_type <> 'ADJUST' THEN 1 END) / COUNT(*), 1) AS pct_move_type_adjust
    FROM profiled
    GROUP BY DATE_TRUNC('MONTH', moved_at), move_operator_count, operator
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(pct_move_type_adjust / NULLIF(min_unit_price, 0), 3) AS pct_move_type_adjust_per_min_unit_price,
        ROUND(move_operator_count / NULLIF(min_unit_price, 0), 3) AS move_operator_count_per_min_unit_price
    FROM totals
)
SELECT *
FROM (
    SELECT move_operator_count, moved_at_month, operator, pct_move_type_adjust
    FROM annotated
) src
PIVOT (
    MAX(pct_move_type_adjust) AS max_v, SUM(pct_move_type_adjust) AS sum_v
    FOR operator IN ('op_bee', 'op_eve', 'op_chan')
)
ORDER BY move_operator_count, moved_at_month
