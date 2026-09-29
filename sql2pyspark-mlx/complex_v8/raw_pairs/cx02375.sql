WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE unit_price <= 372.6
),
enriched AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM scoped s
    JOIN warehouses w ON w.id = s.warehouse_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        warehouse_wh_code,
        warehouse_capacity,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at))) AS distinct_move_type_in_moved_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY moved_at ASC), 3) AS moved_at_cume_dist,
        SIZE(COLLECT_SET(warehouse_id) OVER w_moved_at_month) AS distinct_warehouse_id_in_moved_at_month,
        LAST_VALUE(moved_at) OVER (PARTITION BY sku_id, warehouse_city ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        SUM(warehouse_capacity) OVER (PARTITION BY sku_id, warehouse_city ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_warehouse_capacity,
        ROUND(AVG(warehouse_capacity) OVER (ORDER BY moved_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_2_prior,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY move_type), 2) AS warehouse_capacity_vs_move_type_avg,
        NTH_VALUE(warehouse_capacity, 2) OVER (ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_capacity,
        ROUND(AVG(warehouse_capacity) OVER (ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_7
    FROM enriched
    WINDOW w_moved_at_month AS (PARTITION BY DATE_TRUNC('MONTH', moved_at))
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_distinct_move_type_in_moved AS distinct_warehouse_id_in_moved_at_month_dense_ra,
        COUNT(*) OVER w_distinct_move_type_in_moved_2 AS rows_in_distinct_move_type_in_moved_at_month_war,
        ROUND(AVG(second_warehouse_capacity) OVER (PARTITION BY warehouse_id ORDER BY distinct_move_type_in_moved_at_month, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS second_warehouse_capacity_avg_3_prior,
        ROUND(AVG(warehouse_capacity_avg_7) OVER (PARTITION BY distinct_move_type_in_moved_at_month, distinct_warehouse_id_in_moved_at_month ORDER BY distinct_warehouse_id_in_moved_at_month DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_7_avg_3,
        MIN(running_warehouse_capacity) OVER (PARTITION BY distinct_move_type_in_moved_at_month, warehouse_id) AS distinct_move_type_in_moved_at_month_warehouse_i,
        ROUND(second_warehouse_capacity / NULLIF(SUM(second_warehouse_capacity) OVER (PARTITION BY distinct_move_type_in_moved_at_month, warehouse_id), 0), 4) AS second_warehouse_capacity_share_of_distinct_move,
        SUM(second_warehouse_capacity) OVER (PARTITION BY warehouse_id ORDER BY distinct_move_type_in_moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_second_warehouse_capacity
    FROM sequenced
    WINDOW w_distinct_move_type_in_moved AS (PARTITION BY distinct_move_type_in_moved_at_month, distinct_warehouse_id_in_moved_at_month ORDER BY distinct_warehouse_id_in_moved_at_month DESC NULLS FIRST), w_distinct_move_type_in_moved_2 AS (PARTITION BY distinct_move_type_in_moved_at_month, warehouse_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    warehouse_wh_code,
    warehouse_capacity,
    distinct_move_type_in_moved_at_month,
    moved_at_cume_dist,
    distinct_warehouse_id_in_moved_at_month,
    last_moved_at,
    warehouse_capacity_vs_move_type_avg,
    second_warehouse_capacity,
    rows_in_distinct_move_type_in_moved_at_month_war,
    second_warehouse_capacity_avg_3_prior,
    warehouse_capacity_avg_7_avg_3,
    distinct_move_type_in_moved_at_month_warehouse_i,
    running_second_warehouse_capacity,
    LENGTH(TRIM(warehouse_wh_code)) AS warehouse_wh_code_len,
    CASE WHEN warehouse_capacity >= 11005 THEN 'large' WHEN warehouse_capacity >= 4912 THEN 'medium' ELSE 'small' END AS warehouse_capacity_tier
FROM windowed
ORDER BY id DESC
