WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        MONTH(moved_at) AS moved_at_month,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(unit_price) OVER (ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        MIN(moved_at_month) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_min_moved_at_month,
        ROUND(moved_at_month - AVG(moved_at_month) OVER (PARTITION BY move_type, warehouse_id), 2) AS moved_at_month_vs_move_type_warehouse_id_avg,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, warehouse_id)) AS distinct_sku_id_in_move_type_warehouse_id,
        DENSE_RANK() OVER w_all_by_moved_at AS moved_at_dense_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, moved_at_month), 2) AS unit_price_vs_warehouse_id_moved_at_month_avg,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at, id), 3) AS moved_at_cume_dist,
        COUNT(*) OVER (PARTITION BY warehouse_id, moved_at_month) AS rows_in_warehouse_id_moved_at_month
    FROM filtered
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        moved_at_month,
        move_type_or_unknown,
        running_unit_price,
        distinct_sku_id_in_move_type_warehouse_id,
        moved_at_dense_rank,
        moved_at_cume_dist,
        rows_in_warehouse_id_moved_at_month,
        ROW_NUMBER() OVER (PARTITION BY sku_id, distinct_sku_id_in_move_type_warehouse_id ORDER BY move_type_warehouse_id_min_moved_at_month DESC, id) AS rn,
        MAX(unit_price) OVER (PARTITION BY distinct_sku_id_in_move_type_warehouse_id) AS distinct_sku_id_in_move_type_warehouse_id_max_un,
        ROUND(AVG(moved_at_cume_dist) OVER (PARTITION BY warehouse_id, move_type ORDER BY unit_price_vs_warehouse_id_moved_at_month_avg, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moved_at_cume_dist_avg_7,
        NTILE(3) OVER (PARTITION BY move_type_or_unknown ORDER BY distinct_sku_id_in_move_type_warehouse_id DESC, id) AS distinct_sku_id_in_move_type_warehouse_id_tercil,
        LAST_VALUE(moved_at_cume_dist) IGNORE NULLS OVER (PARTITION BY warehouse_id, move_type ORDER BY unit_price_vs_warehouse_id_moved_at_month_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_cume_dist,
        SIZE(COLLECT_SET(move_type_or_unknown) OVER (PARTITION BY distinct_sku_id_in_move_type_warehouse_id)) AS distinct_move_type_or_unknown_in_distinct_sku_id,
        ROUND(AVG(running_unit_price) OVER (PARTITION BY distinct_sku_id_in_move_type_warehouse_id), 2) AS distinct_sku_id_in_move_type_warehouse_id_avg_ru,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type_or_unknown ORDER BY distinct_sku_id_in_move_type_warehouse_id DESC), 3) AS distinct_sku_id_in_move_type_warehouse_id_percen
    FROM with_trends
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price,
    moved_at_month,
    move_type_or_unknown,
    running_unit_price,
    moved_at_dense_rank,
    rn,
    distinct_sku_id_in_move_type_warehouse_id_max_un,
    moved_at_cume_dist_avg_7,
    last_known_moved_at_cume_dist,
    distinct_sku_id_in_move_type_warehouse_id_percen,
    DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
FROM with_windows
ORDER BY moved_at_dense_rank, id
LIMIT 10
