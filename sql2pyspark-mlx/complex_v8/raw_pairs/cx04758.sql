WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-12 14:00:00'
      AND qty <= 314
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT s.*, w.capacity AS warehouse_capacity, w.wh_code AS warehouse_wh_code
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        warehouse_capacity,
        warehouse_wh_code,
        FIRST_VALUE(warehouse_capacity) IGNORE NULLS OVER (PARTITION BY operator, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_warehouse_capacity,
        warehouse_capacity - LAG(warehouse_capacity) OVER (PARTITION BY sku_id ORDER BY qty, id) AS warehouse_capacity_change,
        ROUND(CUME_DIST() OVER (ORDER BY qty DESC), 3) AS qty_cume_dist,
        SIZE(COLLECT_SET(sku_id) OVER w_operator) AS distinct_sku_id_in_operator,
        ROUND(PERCENT_RANK() OVER w_all_by_qty_ties, 3) AS qty_percent_rank,
        SUM(unit_price) OVER (PARTITION BY operator) AS operator_sum_unit_price,
        ROW_NUMBER() OVER w_warehouse_id_move_type_by_wa AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY qty), 3) AS qty_percent_rank_2,
        LAST_VALUE(warehouse_capacity) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_warehouse_capacity,
        MIN(unit_price) OVER (PARTITION BY operator) AS operator_min_unit_price,
        MAX(qty) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_max_7,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator
    FROM with_parent_warehouse
    WINDOW w_operator AS (PARTITION BY operator), w_all_by_qty_ties AS (ORDER BY qty DESC), w_warehouse_id_move_type_by_wa AS (PARTITION BY warehouse_id, move_type ORDER BY warehouse_capacity DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        COUNT(*) AS cnt,
        SUM(qty_percent_rank) AS total_qty_percent_rank,
        ROUND(STDDEV(qty_max_7), 2) AS stddev_qty_max_7,
        MIN(last_warehouse_capacity) AS min_last_warehouse_capacity
    FROM with_trends
    WHERE last_warehouse_capacity <= 11005
    GROUP BY warehouse_id
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        warehouse_id,
        total_qty_percent_rank,
        stddev_qty_max_7,
        min_last_warehouse_capacity,
        ROUND(AVG(total_qty_percent_rank) OVER (PARTITION BY cnt, warehouse_id), 2) AS cnt_warehouse_id_avg_total_qty_percent_rank,
        RANK() OVER (ORDER BY cnt) AS cnt_rank
    FROM totals
)
-- Final projection
SELECT
    warehouse_id,
    total_qty_percent_rank,
    stddev_qty_max_7,
    cnt_warehouse_id_avg_total_qty_percent_rank
FROM metrics
ORDER BY cnt_warehouse_id_avg_total_qty_percent_rank
