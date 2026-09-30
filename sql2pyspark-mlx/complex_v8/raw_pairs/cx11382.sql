WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE (unit_price < 396.4 OR HOUR(moved_at) BETWEEN 9 AND 14)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at_label,
        days_since_moved_at,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, move_type)) AS distinct_warehouse_id_in_sku_id_move_type,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id, move_type), 2) AS qty_vs_sku_id_move_type_avg,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        LAST_VALUE(days_since_moved_at) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_moved_at,
        ROUND(AVG(unit_price) OVER (ORDER BY days_since_moved_at ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_4,
        MIN(unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_min_unit_price,
        MAX(qty) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_max_qty,
        NTILE(3) OVER (ORDER BY unit_price DESC, id) AS unit_price_tercile,
        LEAD(unit_price, 1, 0) OVER w_all_by_days_since_moved_at AS next_unit_price,
        RANK() OVER (ORDER BY unit_price DESC, id) AS unit_price_rank,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty
    FROM recent
    WINDOW w_all_by_days_since_moved_at AS (ORDER BY days_since_moved_at ASC, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(next_unit_price - 289.7) AS next_unit_price_distance,
        GREATEST(unit_price_tercile, unit_price_rank) AS greatest_unit_price_tercile_unit_price_rank,
        LOWER(TRIM(moved_at_label)) AS moved_at_label_clean
    FROM sequenced
    WHERE moved_at_label IS NOT NULL
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(days_since_moved_at) OVER (PARTITION BY unit_price_tercile) AS unit_price_tercile_max_days_since_moved_at,
        LAG(sku_id_move_type_max_qty) OVER (PARTITION BY sku_id, unit_price_rank ORDER BY greatest_unit_price_tercile_unit_price_rank DESC, id) AS prev_sku_id_move_type_max_qty,
        FIRST_VALUE(next_unit_price) OVER (PARTITION BY unit_price_tercile ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_unit_price,
        ROUND(next_unit_price_distance - AVG(next_unit_price_distance) OVER (PARTITION BY unit_price_tercile), 2) AS next_unit_price_distance_vs_unit_price_tercile_a,
        ROUND(PERCENT_RANK() OVER w_sku_id_unit_price_rank_by_gr, 3) AS greatest_unit_price_tercile_unit_price_rank_perc,
        SUM(unit_price_tercile) OVER (PARTITION BY unit_price_tercile) AS unit_price_tercile_sum_unit_price_tercile
    FROM labeled
    WINDOW w_sku_id_unit_price_rank_by_gr AS (PARTITION BY sku_id, unit_price_rank ORDER BY greatest_unit_price_tercile_unit_price_rank DESC, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at_label,
    distinct_warehouse_id_in_sku_id_move_type,
    unit_price_avg_4,
    sku_id_move_type_max_qty,
    unit_price_tercile,
    next_unit_price,
    unit_price_rank,
    greatest_unit_price_tercile_unit_price_rank,
    moved_at_label_clean,
    unit_price_tercile_max_days_since_moved_at,
    prev_sku_id_move_type_max_qty,
    first_next_unit_price,
    greatest_unit_price_tercile_unit_price_rank_perc,
    unit_price_tercile_sum_unit_price_tercile
FROM sequenced_2
WHERE sku_id_move_type_min_unit_price BETWEEN 1500.7 AND 3250.3
ORDER BY first_next_unit_price, unit_price_tercile
