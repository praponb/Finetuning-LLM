WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
enriched AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY MONTH(warehouse_opened_on) ORDER BY moved_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        LAG(unit_price, 1, 0) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id) AS prev_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at_year ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_avg_2,
        LAST_VALUE(warehouse_opened_on) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_warehouse_opened_on,
        MAX(unit_price) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        COUNT(*) OVER w_moved_at_year_to_date AS rows_in_moved_at_year_to_date,
        LAG(warehouse_opened_on, 3) OVER w_sku_id_move_type_by_qty AS prev3_warehouse_opened_on,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id, move_type ORDER BY qty DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        COUNT(*) OVER (PARTITION BY move_type, sku_id) AS rows_in_move_type_sku_id,
        MIN(unit_price) OVER (PARTITION BY warehouse_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type_sku_id) AS distinct_warehouse_id_in_move_type_sku_id,
        NTH_VALUE(qty, 2) OVER (PARTITION BY sku_id, move_type ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        MAX(qty) OVER (PARTITION BY MONTH(warehouse_opened_on) ORDER BY moved_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        ROUND(AVG(unit_price) OVER w_move_type_sku_id, 2) AS move_type_sku_id_avg_unit_price
    FROM enriched
    WINDOW w_moved_at_year_to_date AS (PARTITION BY TO_DATE(moved_at_year)), w_sku_id_move_type_by_qty AS (PARTITION BY sku_id, move_type ORDER BY qty DESC, id), w_move_type_sku_id AS (PARTITION BY move_type, sku_id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        moved_at_year,
        warehouse_opened_on,
        running_unit_price,
        prev_unit_price,
        qty_avg_2,
        last_warehouse_opened_on,
        prev3_warehouse_opened_on,
        qty_avg_1_prior,
        rows_in_move_type_sku_id,
        min_unit_price_so_far,
        max_qty_so_far,
        move_type_sku_id_avg_unit_price,
        COALESCE(qty_avg_2, 0.0) AS qty_avg_2_filled,
        CASE WHEN operator IN ('op_bee', 'op_chan') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group,
        ABS(qty_avg_1_prior - 350.3) AS qty_avg_1_prior_distance
    FROM with_windows
    WHERE running_unit_price < 3000.4
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(min_unit_price_so_far) OVER (PARTITION BY warehouse_id ORDER BY max_qty_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_unit_price_so_far,
        SUM(qty_avg_1_prior_distance) OVER (PARTITION BY sku_id) AS sku_id_sum_qty_avg_1_prior_distance,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator_group)) AS distinct_warehouse_id_in_operator_group,
        LEAD(warehouse_opened_on) OVER (ORDER BY running_unit_price, id) AS next_warehouse_opened_on,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at_year ASC, id) AS moved_at_year_dense_rank,
        COUNT(*) OVER (PARTITION BY warehouse_id ORDER BY max_qty_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', prev3_warehouse_opened_on), sku_id ORDER BY prev_unit_price ASC), 3) AS prev_unit_price_cume_dist
    FROM tagged
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    unit_price,
    warehouse_opened_on,
    running_unit_price,
    prev_unit_price,
    qty_avg_2,
    last_warehouse_opened_on,
    prev3_warehouse_opened_on,
    min_unit_price_so_far,
    max_qty_so_far,
    move_type_sku_id_avg_unit_price,
    qty_avg_2_filled,
    operator_group,
    qty_avg_1_prior_distance,
    sku_id_sum_qty_avg_1_prior_distance,
    qty_vs_sku_id_avg,
    moved_at_year_dense_rank,
    running_count
FROM metrics
ORDER BY sku_id
