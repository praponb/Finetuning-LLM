WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE unit_price <= 396.4
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        s.move_type_ref,
        w.wh_code AS warehouse_wh_code
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at DESC), 3) AS moved_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        DENSE_RANK() OVER (ORDER BY moved_at) AS moved_at_dense_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator, MONTH(moved_at))) AS distinct_move_type_in_operator_moved_at_month,
        LAG(unit_price, 2) OVER w_all_by_moved_at AS prev2_unit_price,
        ROW_NUMBER() OVER (ORDER BY moved_at, id) AS moved_at_seq,
        LEAD(qty) OVER w_move_type_moved_at_to_date_b_2 AS next_qty,
        ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price, id) AS unit_price_seq,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, MONTH(moved_at)), 2) AS operator_moved_at_month_avg_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_avg_7,
        COUNT(*) OVER (PARTITION BY operator, MONTH(moved_at)) AS rows_in_operator_moved_at_month,
        MAX(unit_price) OVER (PARTITION BY operator, MONTH(moved_at)) AS operator_moved_at_month_max_unit_price,
        LEAD(unit_price) OVER (ORDER BY moved_at, id) AS next_unit_price
    FROM with_details
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id), w_move_type_moved_at_to_date_b_2 AS (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at DESC, id), w_move_type_moved_at_to_date_b AS (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at DESC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE moved_at_dense_rank <= 3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY rows_in_operator_moved_at_month ASC, id), 3) AS rows_in_operator_moved_at_month_cume_dist,
        LEAD(next_unit_price) OVER w_all_by_next_unit_price AS next_next_unit_price,
        ROUND(minutes_since_prev_moved_at / NULLIF(SUM(minutes_since_prev_moved_at) OVER w_move_type, 0), 4) AS minutes_since_prev_moved_at_share_of_move_type
    FROM top_per_group
    WINDOW w_all_by_next_unit_price AS (ORDER BY next_unit_price DESC, id), w_move_type AS (PARTITION BY move_type)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    operator,
    unit_price,
    move_type_ref,
    warehouse_wh_code,
    moved_at_cume_dist,
    minutes_since_prev_moved_at,
    moved_at_dense_rank,
    distinct_move_type_in_operator_moved_at_month,
    prev2_unit_price,
    moved_at_seq,
    next_qty,
    unit_price_seq,
    operator_moved_at_month_avg_unit_price,
    qty_avg_7,
    rows_in_operator_moved_at_month,
    operator_moved_at_month_max_unit_price,
    next_unit_price,
    rows_in_operator_moved_at_month_cume_dist,
    minutes_since_prev_moved_at_share_of_move_type,
    CASE WHEN move_type IN ('ADJUST', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
    DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label_2
FROM sequenced
ORDER BY prev2_unit_price DESC, moved_at_label_2 NULLS LAST
