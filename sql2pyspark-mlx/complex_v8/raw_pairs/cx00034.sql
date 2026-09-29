WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE qty BETWEEN 137 AND 300
      AND sku_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        days_since_moved_at,
        moved_at_label,
        SUM(unit_price) OVER (PARTITION BY sku_id) AS sku_id_sum_unit_price,
        DENSE_RANK() OVER w_all_by_days_since_moved_at_t AS days_since_moved_at_dense_rank,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        LAG(days_since_moved_at) OVER w_all_by_days_since_moved_at AS prev_days_since_moved_at,
        MIN(days_since_moved_at) OVER (PARTITION BY sku_id) AS sku_id_min_days_since_moved_at,
        COUNT(*) OVER (ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id), 0), 4) AS unit_price_share_of_sku_id,
        MAX(unit_price) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        SUM(days_since_moved_at) OVER (PARTITION BY sku_id) AS sku_id_sum_days_since_moved_at,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_move_type_operator, 0), 4) AS unit_price_share_of_move_type_operator,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        ROW_NUMBER() OVER (ORDER BY days_since_moved_at DESC, id) AS days_since_moved_at_seq,
        COUNT(*) OVER w_move_type_operator AS rows_in_move_type_operator,
        LAST_VALUE(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
    FROM cleaned
    WINDOW w_all_by_days_since_moved_at_t AS (ORDER BY days_since_moved_at DESC), w_all_by_days_since_moved_at AS (ORDER BY days_since_moved_at, id), w_move_type_operator AS (PARTITION BY move_type, operator), w_sku_id AS (PARTITION BY sku_id)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    operator,
    sku_id_sum_unit_price,
    days_since_moved_at_dense_rank,
    second_unit_price,
    prev_days_since_moved_at,
    sku_id_min_days_since_moved_at,
    running_count,
    unit_price_share_of_sku_id,
    sku_id_max_unit_price,
    sku_id_sum_days_since_moved_at,
    rows_in_sku_id,
    days_since_moved_at_seq,
    rows_in_move_type_operator,
    last_unit_price,
    GREATEST(last_unit_price, sku_id_max_unit_price) AS greatest_last_unit_price_sku_id_max_unit_price
FROM metrics
ORDER BY last_unit_price, id
LIMIT 10
