WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE operator NOT IN ('op_chan', 'op_eve')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_qty_filled AS qty_filled_seq,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        SUM(qty_filled) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_filled,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator), 0), 4) AS unit_price_share_of_operator,
        LAG(qty_filled, 3) OVER (PARTITION BY sku_id ORDER BY unit_price, id) AS prev3_qty_filled,
        unit_price - LAG(unit_price) OVER (ORDER BY moved_at, id) AS unit_price_change,
        RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at ASC, id) AS moved_at_rank,
        FIRST_VALUE(qty_filled) IGNORE NULLS OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty_filled,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at ASC) AS moved_at_dense_rank,
        LEAD(moved_at) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at ASC, id) AS next_moved_at,
        MIN(qty_filled) OVER (PARTITION BY operator) AS operator_min_qty_filled
    FROM scoped
    WINDOW w_all_by_qty_filled AS (ORDER BY qty_filled, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator_move_type_label,
        qty_filled_seq,
        distinct_warehouse_id_in_sku_id,
        running_qty_filled,
        prev3_qty_filled,
        unit_price_change,
        moved_at_rank,
        first_known_qty_filled,
        moved_at_dense_rank,
        next_moved_at,
        operator_min_qty_filled,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        CASE WHEN running_qty_filled >= 3000 THEN 'large' WHEN running_qty_filled >= 2251 THEN 'medium' ELSE 'small' END AS running_qty_filled_tier
    FROM metrics
    WHERE next_moved_at < TIMESTAMP '2025-02-24 08:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(prev3_qty_filled) OVER (ORDER BY moved_at_day DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev3_qty_filled_max_2,
        NTH_VALUE(next_moved_at, 2) OVER (PARTITION BY move_type, running_qty_filled_tier ORDER BY moved_at_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_moved_at,
        ROW_NUMBER() OVER (PARTITION BY move_type, running_qty_filled_tier ORDER BY moved_at_rank DESC NULLS FIRST, id) AS seq_no,
        ROUND(first_known_qty_filled - AVG(first_known_qty_filled) OVER w_warehouse_id, 2) AS first_known_qty_filled_vs_warehouse_id_avg,
        ROUND(running_qty_filled - AVG(running_qty_filled) OVER w_warehouse_id, 2) AS running_qty_filled_vs_warehouse_id_avg,
        SUM(unit_price_change) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_unit_price_change,
        ROUND(CUME_DIST() OVER (ORDER BY prev3_qty_filled DESC), 3) AS prev3_qty_filled_cume_dist,
        LEAD(next_moved_at) OVER (PARTITION BY move_type, running_qty_filled_tier ORDER BY moved_at_rank DESC NULLS FIRST, id) AS next_next_moved_at
    FROM tagged
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator_move_type_label,
    distinct_warehouse_id_in_sku_id,
    running_qty_filled,
    prev3_qty_filled,
    unit_price_change,
    moved_at_rank,
    first_known_qty_filled,
    moved_at_dense_rank,
    next_moved_at,
    operator_min_qty_filled,
    prev3_qty_filled_max_2,
    second_next_moved_at,
    seq_no,
    running_qty_filled_vs_warehouse_id_avg,
    prev3_qty_filled_cume_dist,
    CONCAT_WS(' / ', operator_move_type_label, move_type) AS operator_move_type_label_move_type_label,
    DATEDIFF(DATE '2025-12-31', next_next_moved_at) AS days_since_next_next_moved_at
FROM with_windows
ORDER BY prev3_qty_filled
