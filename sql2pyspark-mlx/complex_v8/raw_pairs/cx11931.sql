WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, YEAR(moved_at)), 2) AS unit_price_vs_warehouse_id_moved_at_year_avg,
        NTILE(10) OVER w_warehouse_id_by_qty AS qty_decile,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id) AS rn,
        LAST_VALUE(moved_at_week) IGNORE NULLS OVER (PARTITION BY MONTH(moved_at), warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_week,
        FIRST_VALUE(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        MIN(unit_price) OVER (PARTITION BY warehouse_id, YEAR(moved_at)) AS warehouse_id_moved_at_year_min_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at ASC), 3) AS moved_at_cume_dist
    FROM recent_moves
    WINDOW w_warehouse_id_by_qty AS (PARTITION BY warehouse_id ORDER BY qty DESC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(last_known_moved_at_week) AS last_known_moved_at_week_date,
        ABS(rn - 6) AS rn_distance,
        ROUND(first_unit_price / NULLIF(qty_decile, 0), 3) AS first_unit_price_per_qty_decile
    FROM ranked
    WHERE minutes_since_prev_moved_at <= 4250.0
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price,
        operator_move_type_label,
        moved_at_week,
        unit_price_vs_warehouse_id_moved_at_year_avg,
        qty_decile,
        second_qty,
        minutes_since_prev_moved_at,
        last_known_moved_at_week,
        warehouse_id_moved_at_year_min_unit_price,
        last_known_moved_at_week_date,
        rn_distance,
        first_unit_price_per_qty_decile,
        SIZE(COLLECT_SET(warehouse_id) OVER w_rn_distance) AS distinct_warehouse_id_in_rn_distance,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY rn_distance)) AS distinct_sku_id_in_rn_distance,
        ROUND(minutes_since_prev_moved_at / NULLIF(SUM(minutes_since_prev_moved_at) OVER (PARTITION BY rn_distance), 0), 4) AS minutes_since_prev_moved_at_share_of_rn_distance
    FROM tagged
    WINDOW w_rn_distance AS (PARTITION BY rn_distance)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    unit_price,
    moved_at_week,
    second_qty,
    minutes_since_prev_moved_at,
    last_known_moved_at_week,
    warehouse_id_moved_at_year_min_unit_price,
    last_known_moved_at_week_date,
    rn_distance,
    first_unit_price_per_qty_decile,
    distinct_warehouse_id_in_rn_distance,
    minutes_since_prev_moved_at_share_of_rn_distance
FROM metrics
ORDER BY last_known_moved_at_week, moved_at_week
