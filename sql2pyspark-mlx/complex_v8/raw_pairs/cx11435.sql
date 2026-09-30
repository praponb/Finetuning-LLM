WITH recent AS (
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
        CASE WHEN qty >= 249 THEN 'HIGH' WHEN qty >= 152 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND moved_at >= TIMESTAMP '2025-01-31 10:00:00'
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.qty_tier,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active,
        w.city AS warehouse_city
    FROM recent s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.is_active = TRUE
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        sku_category,
        sku_labels,
        warehouse_city,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(moved_at), move_type ORDER BY moved_at, id), 3) AS moved_at_cume_dist,
        ROW_NUMBER() OVER w_sku_category_sku_is_active_b AS qty_seq,
        ROUND(AVG(qty) OVER (PARTITION BY sku_is_active, warehouse_city), 2) AS sku_is_active_warehouse_city_avg_qty,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at, id), 3) AS moved_at_cume_dist_2,
        MAX(qty) OVER (PARTITION BY sku_id) AS sku_id_max_qty,
        RANK() OVER (PARTITION BY MONTH(moved_at), move_type ORDER BY moved_at) AS moved_at_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_is_active_warehouse_city) AS distinct_warehouse_id_in_sku_is_active_warehouse,
        SUM(qty) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM joined_sku
    WINDOW w_sku_category_sku_is_active_b AS (PARTITION BY sku_category, sku_is_active ORDER BY qty NULLS LAST, id), w_sku_is_active_warehouse_city AS (PARTITION BY sku_is_active, warehouse_city)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        sku_labels,
        moved_at_cume_dist,
        qty_seq,
        distinct_warehouse_id_in_sku_id,
        moved_at_rank,
        distinct_warehouse_id_in_sku_is_active_warehouse,
        running_qty,
        COUNT(*) OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY distinct_warehouse_id_in_sku_is_active_warehouse, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_city, warehouse_id ORDER BY distinct_warehouse_id_in_sku_is_active_warehouse), 3) AS distinct_warehouse_id_in_sku_is_active_warehouse_2,
        SIZE(COLLECT_SET(warehouse_city) OVER w_moved_at_rank_qty_seq) AS distinct_warehouse_city_in_moved_at_rank_qty_seq,
        MIN(running_qty) OVER (PARTITION BY moved_at_rank, qty_seq) AS moved_at_rank_qty_seq_min_running_qty,
        qty - LAG(qty) OVER (PARTITION BY distinct_warehouse_id_in_sku_id, sku_id ORDER BY running_qty DESC, id) AS qty_change,
        SIZE(COLLECT_SET(sku_id) OVER w_moved_at_rank_qty_seq) AS distinct_sku_id_in_moved_at_rank_qty_seq
    FROM with_trends
    WINDOW w_moved_at_rank_qty_seq AS (PARTITION BY moved_at_rank, qty_seq)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_warehouse_id_in_sku_id,
        SUM(distinct_sku_id_in_moved_at_rank_qty_seq) AS total_distinct_sku_id_in_moved_at_rank_qty_seq,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ROUND(AVG(running_qty), 2) AS avg_running_qty,
        COUNT(*) AS row_count,
        MIN(moved_at_rank) AS min_moved_at_rank,
        ROUND(AVG(distinct_warehouse_id_in_sku_id), 2) AS avg_distinct_warehouse_id_in_sku_id
    FROM sequenced
    WHERE distinct_warehouse_id_in_sku_id >= 4
    GROUP BY 1
)
-- Final projection
SELECT
    distinct_warehouse_id_in_sku_id,
    avg_running_qty,
    avg_distinct_warehouse_id_in_sku_id
FROM rollup_base
WHERE avg_distinct_warehouse_id_in_sku_id BETWEEN 2.8 AND 9.1
ORDER BY avg_running_qty, avg_distinct_warehouse_id_in_sku_id
