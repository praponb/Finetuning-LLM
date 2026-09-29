WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        MONTH(moved_at) AS moved_at_month
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.moved_at_month,
        sk.category AS sku_category,
        sk.sku_code
    FROM prepared_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        unit_price,
        sku_category,
        sku_code,
        COUNT(*) OVER w_moved_at_month AS rows_in_moved_at_month,
        ROUND(qty - AVG(qty) OVER (PARTITION BY moved_at_month), 2) AS qty_vs_moved_at_month_avg,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY moved_at_month)) AS distinct_move_type_in_moved_at_month,
        ROUND(AVG(moved_at_month) OVER (PARTITION BY moved_at_month), 2) AS moved_at_month_avg_moved_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, moved_at_month ORDER BY unit_price), 3) AS unit_price_cume_dist,
        SUM(qty) OVER (PARTITION BY moved_at_month) AS moved_at_month_sum_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY moved_at_month), 2) AS moved_at_month_avg_unit_price,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id, moved_at_month ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price
    FROM combined_details_sku
    WINDOW w_moved_at_month AS (PARTITION BY moved_at_month)
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    unit_price,
    sku_category,
    rows_in_moved_at_month,
    qty_vs_moved_at_month_avg,
    distinct_move_type_in_moved_at_month,
    moved_at_month_avg_moved_at_month,
    unit_price_cume_dist,
    second_unit_price,
    ABS(moved_at_month_sum_qty - 2500) AS moved_at_month_sum_qty_distance,
    ROUND(unit_price_cume_dist * 1.07, 2) AS unit_price_cume_dist_adj
FROM sequenced
WHERE unit_price_cume_dist >= 0.2
ORDER BY sku_id DESC, rows_in_moved_at_month
