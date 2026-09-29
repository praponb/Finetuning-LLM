WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day_2
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.moved_at_day,
        s.moved_at_day_2,
        w.opened_on AS warehouse_opened_on,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM filtered_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        moved_at_day,
        moved_at_day_2,
        sku_unit_cost,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY MONTH(warehouse_opened_on), sku_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type, sku_category ORDER BY moved_at), 3) AS moved_at_cume_dist,
        MIN(qty) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        LAST_VALUE(moved_at_day_2) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY moved_at_day_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_day_2,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_category, sku_id)) AS distinct_move_type_in_sku_category_sku_id,
        SUM(qty) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_sum_1_prior,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_category, sku_id)) AS distinct_warehouse_id_in_sku_category_sku_id,
        LAST_VALUE(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at_day_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER w_sku_category, 0), 4) AS sku_unit_cost_share_of_sku_category
    FROM with_details_warehouse
    WINDOW w_sku_category AS (PARTITION BY sku_category)
),
enriched AS (
    -- Derive helper columns
    SELECT *, moved_at_day + INTERVAL 2 HOURS AS moved_at_day_plus_2h
    FROM sequenced
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    moved_at_day,
    sku_unit_cost,
    sku_unit_cost_avg_1_prior,
    moved_at_cume_dist,
    min_qty_so_far,
    last_known_moved_at_day_2,
    distinct_move_type_in_sku_category_sku_id,
    distinct_warehouse_id_in_sku_category_sku_id,
    moved_at_day_plus_2h,
    DATE_FORMAT(moved_at_day_2, 'yyyy-MM-dd') AS moved_at_day_2_label
FROM enriched
WHERE qty <= 387
ORDER BY distinct_move_type_in_sku_category_sku_id NULLS LAST
