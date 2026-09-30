WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT s.*, w.wh_code AS warehouse_wh_code
    FROM prepared s
    JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type_sku_id) AS distinct_warehouse_id_in_move_type_sku_id,
        ROUND(CUME_DIST() OVER w_all_by_qty, 3) AS qty_cume_dist,
        LAG(moved_at, 2) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id) AS prev2_moved_at,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        MAX(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        MAX(qty) OVER w_move_type_warehouse_id AS move_type_warehouse_id_max_qty,
        MAX(qty) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_max_3_prior,
        LEAD(moved_at, 3) OVER (ORDER BY qty DESC NULLS FIRST, id) AS next3_moved_at,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty, id) AS rn
    FROM with_parent_warehouse
    WINDOW w_move_type_sku_id AS (PARTITION BY move_type, sku_id), w_all_by_qty AS (ORDER BY qty DESC NULLS FIRST, id), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        sku_id_avg_qty,
        distinct_warehouse_id_in_move_type_sku_id,
        qty_cume_dist,
        prev2_moved_at,
        qty_avg_4,
        max_qty_so_far,
        move_type_warehouse_id_max_qty,
        next3_moved_at,
        NTILE(3) OVER (PARTITION BY distinct_warehouse_id_in_move_type_sku_id ORDER BY qty_cume_dist DESC, id) AS qty_cume_dist_tercile,
        MIN(second_qty) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_min_second_qty,
        ROUND(AVG(sku_id_avg_qty) OVER (PARTITION BY rn, move_type ORDER BY max_qty_so_far DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS sku_id_avg_qty_avg_4,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, sku_id)) AS distinct_move_type_in_warehouse_id_sku_id,
        ROUND(PERCENT_RANK() OVER w_rn_move_type_by_max_qty_so_f_2, 3) AS max_qty_so_far_percent_rank,
        DENSE_RANK() OVER w_move_type_by_max_qty_so_far AS max_qty_so_far_dense_rank,
        LAG(qty, 3) OVER (PARTITION BY move_type ORDER BY max_qty_so_far DESC NULLS FIRST, id) AS prev3_qty,
        MIN(qty_cume_dist) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_min_qty_cume_dist
    FROM ranked
    WINDOW w_rn_move_type_by_max_qty_so_f_2 AS (PARTITION BY rn, move_type ORDER BY max_qty_so_far DESC, id), w_move_type_by_max_qty_so_far AS (PARTITION BY move_type ORDER BY max_qty_so_far DESC NULLS FIRST)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        CASE WHEN sku_id_avg_qty >= 325.4 THEN 'A' WHEN sku_id_avg_qty >= 200.6 THEN 'B' ELSE 'C' END AS sku_id_avg_qty_tier,
        warehouse_id,
        MAX(next3_moved_at) AS last_next3_moved_at,
        MAX(qty_avg_4) AS max_qty_avg_4,
        COUNT(*) AS n_rows
    FROM ranked_2
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
ranked_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(n_rows) OVER w_warehouse_id_sku_id AS warehouse_id_sku_id_min_n_rows,
        LAG(last_next3_moved_at, 3) OVER w_all_by_n_rows AS prev3_last_next3_moved_at
    FROM totals
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_all_by_n_rows AS (ORDER BY n_rows DESC, sku_id, sku_id_avg_qty_tier, warehouse_id)
)
SELECT
    sku_id,
    warehouse_id,
    max_qty_avg_4,
    warehouse_id_sku_id_min_n_rows,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
    CASE WHEN n_rows >= 9 THEN 'A' WHEN n_rows >= 5 THEN 'B' ELSE 'C' END AS n_rows_tier
FROM ranked_3
WHERE TO_DATE(prev3_last_next3_moved_at) < DATE '2025-02-18'
ORDER BY n_rows_tier
