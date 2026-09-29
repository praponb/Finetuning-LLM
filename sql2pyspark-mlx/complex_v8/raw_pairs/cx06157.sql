WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator IS NOT NULL
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_move_type_label,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND LOWER(w.wh_code) LIKE '%ku%'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        operator_move_type_label,
        warehouse_wh_code,
        warehouse_capacity,
        RANK() OVER w_all_by_qty_ties AS qty_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_qty, 3) AS qty_percent_rank,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        qty - LAG(qty) OVER (ORDER BY moved_at DESC, id) AS qty_change,
        COUNT(*) OVER w_sku_id_warehouse_id AS rows_in_sku_id_warehouse_id,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id, operator ORDER BY moved_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_4,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY sku_id, TO_DATE(moved_at)), 2) AS warehouse_capacity_vs_sku_id_moved_at_to_date_av,
        SUM(qty) OVER w_warehouse_id_operator AS warehouse_id_operator_sum_qty,
        FIRST_VALUE(qty) OVER (ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, operator), 2) AS warehouse_id_operator_avg_unit_price,
        LEAD(moved_at) OVER w_sku_id_operator_by_moved_at_2 AS next_moved_at,
        MAX(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far
    FROM with_details
    WINDOW w_all_by_qty_ties AS (ORDER BY qty), w_all_by_qty AS (ORDER BY qty, id), w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id), w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator), w_sku_id_operator_by_moved_at_2 AS (PARTITION BY sku_id, operator ORDER BY moved_at, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(second_qty, 3) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', next_moved_at) ORDER BY warehouse_id_operator_sum_qty, id) AS prev3_second_qty,
        NTH_VALUE(warehouse_capacity, 2) OVER (ORDER BY moved_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_capacity,
        ROUND(AVG(qty_rank) OVER w_move_type_qty_rank, 2) AS move_type_qty_rank_avg_qty_rank,
        LAG(max_qty_so_far, 3) OVER w_all_by_moved_at_2 AS prev3_max_qty_so_far,
        SUM(qty) OVER w_move_type_qty_rank AS move_type_qty_rank_sum_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, qty_rank)) AS distinct_sku_id_in_move_type_qty_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_qty_percent_rank_ties, 3) AS qty_percent_rank_percent_rank
    FROM with_windows
    WINDOW w_move_type_qty_rank AS (PARTITION BY move_type, qty_rank), w_all_by_moved_at_2 AS (ORDER BY moved_at ASC NULLS LAST, id), w_all_by_qty_percent_rank_ties AS (ORDER BY qty_percent_rank DESC)
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', next_moved_at) AS next_moved_at_month,
        move_type,
        SUM(CASE WHEN LOWER(warehouse_wh_code) LIKE '%ku%' THEN 1 ELSE 0 END) AS n_lower_warehouse_wh_code_like,
        PERCENTILE_APPROX(qty, 0.5) AS median_qty,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        COUNT(*) FILTER (WHERE UPPER(TRIM(warehouse_wh_code)) LIKE 'B%') AS n_upper_trim_warehouse_wh_code,
        PERCENTILE_APPROX(warehouse_capacity, 0.5) AS median_warehouse_capacity,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', next_moved_at), move_type
    HAVING MAX(move_type_qty_rank_avg_qty_rank) > 3.2
)
SELECT
    next_moved_at_month,
    move_type,
    n_lower_warehouse_wh_code_like,
    median_qty,
    distinct_warehouse_ids,
    median_warehouse_capacity,
    move_type_list,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
    UPPER(TRIM(move_type)) AS move_type_clean
FROM grouped
WHERE next_moved_at_month >= TIMESTAMP '2025-01-17 09:00:00'
ORDER BY median_warehouse_capacity
