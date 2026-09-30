WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        ABS(unit_price - 272.4) AS unit_price_distance,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE move_type <> 'OUTBOUND'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        unit_price_distance,
        days_since_moved_at,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, operator ORDER BY days_since_moved_at DESC), 3) AS days_since_moved_at_cume_dist,
        LEAD(unit_price_distance, 2) OVER (PARTITION BY warehouse_id, operator ORDER BY days_since_moved_at DESC, id) AS next2_unit_price_distance,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, move_type)) AS distinct_warehouse_id_in_operator_move_type,
        COUNT(*) OVER (PARTITION BY operator, move_type) AS rows_in_operator_move_type,
        SUM(days_since_moved_at) OVER w_operator_move_type AS operator_move_type_sum_days_since_moved_at,
        days_since_moved_at - LAG(days_since_moved_at) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price_distance ASC, id) AS days_since_moved_at_change,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator, move_type)) AS distinct_sku_id_in_operator_move_type,
        LAST_VALUE(unit_price_distance) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price_distance,
        MAX(qty) OVER w_operator_move_type AS operator_move_type_max_qty,
        SUM(unit_price_distance) OVER (PARTITION BY warehouse_id, operator ORDER BY days_since_moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_distance,
        FIRST_VALUE(unit_price_distance) IGNORE NULLS OVER (ORDER BY unit_price_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_distance,
        SUM(qty) OVER (PARTITION BY warehouse_id, operator ORDER BY days_since_moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM prepared
    WINDOW w_operator_move_type AS (PARTITION BY operator, move_type)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_distinct_warehouse_id_in_ope AS operator_move_type_sum_days_since_moved_at_seq,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER (PARTITION BY sku_id), 0), 4) AS days_since_moved_at_share_of_sku_id,
        LEAD(operator_move_type_max_qty) OVER (PARTITION BY distinct_warehouse_id_in_operator_move_type, operator ORDER BY operator_move_type_sum_days_since_moved_at DESC, id) AS next_operator_move_type_max_qty,
        NTH_VALUE(days_since_moved_at, 2) OVER (PARTITION BY distinct_warehouse_id_in_operator_move_type, operator ORDER BY last_unit_price_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_moved_at
    FROM with_trends
    WINDOW w_distinct_warehouse_id_in_ope AS (PARTITION BY distinct_warehouse_id_in_operator_move_type, operator ORDER BY operator_move_type_sum_days_since_moved_at DESC, id)
)
SELECT
    id,
    sku_id,
    move_type,
    unit_price_distance,
    days_since_moved_at,
    next2_unit_price_distance,
    rows_in_operator_move_type,
    operator_move_type_sum_days_since_moved_at,
    days_since_moved_at_change,
    distinct_sku_id_in_operator_move_type,
    last_unit_price_distance,
    operator_move_type_max_qty,
    running_unit_price_distance,
    first_known_unit_price_distance,
    running_qty,
    operator_move_type_sum_days_since_moved_at_seq,
    days_since_moved_at_share_of_sku_id,
    next_operator_move_type_max_qty,
    second_days_since_moved_at,
    ROUND(last_unit_price_distance / NULLIF(running_qty, 0), 3) AS last_unit_price_distance_per_running_qty
FROM sequenced
ORDER BY distinct_sku_id_in_operator_move_type DESC NULLS FIRST, days_since_moved_at_share_of_sku_id DESC
