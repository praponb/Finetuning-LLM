WITH recent_moves AS (
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
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        LOWER(TRIM(operator)) AS operator_clean
    FROM stock_moves
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        operator_clean,
        RANK() OVER (PARTITION BY move_type, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at NULLS LAST, id) AS moved_at_rank,
        COUNT(*) OVER w_sku_id_move_type AS rows_in_sku_id_move_type,
        RANK() OVER (ORDER BY unit_price) AS unit_price_rank,
        MAX(unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_max_unit_price,
        LAG(unit_price) OVER (PARTITION BY move_type, DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at NULLS LAST, id) AS prev_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER w_operator) AS distinct_sku_id_in_operator,
        RANK() OVER (PARTITION BY operator ORDER BY qty DESC) AS qty_rank,
        SUM(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM recent_moves
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type), w_operator AS (PARTITION BY operator)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        operator_clean,
        moved_at_rank,
        rows_in_sku_id_move_type,
        unit_price_rank,
        prev_unit_price,
        distinct_sku_id_in_operator,
        running_qty,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM metrics
    WHERE distinct_sku_id_in_operator <= 9
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        operator_clean,
        moved_at_rank,
        rows_in_sku_id_move_type,
        unit_price_rank,
        prev_unit_price,
        distinct_sku_id_in_operator,
        running_qty,
        moved_at_week,
        ROUND(AVG(prev_unit_price) OVER (PARTITION BY unit_price_rank, distinct_sku_id_in_operator), 2) AS unit_price_rank_distinct_sku_id_in_operator_avg_,
        FIRST_VALUE(prev_unit_price) IGNORE NULLS OVER (PARTITION BY operator, distinct_sku_id_in_operator ORDER BY moved_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_unit_price,
        SUM(rows_in_sku_id_move_type) OVER (PARTITION BY unit_price_rank ORDER BY prev_unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rows_in_sku_id_move_type_sum_6_prior,
        LAG(moved_at_week, 3) OVER (PARTITION BY operator, distinct_sku_id_in_operator ORDER BY moved_at_week DESC, id) AS prev3_moved_at_week,
        ROUND(moved_at_rank - AVG(moved_at_rank) OVER w_unit_price_rank_distinct_sku, 2) AS moved_at_rank_vs_unit_price_rank_distinct_sku_id,
        LEAD(moved_at_week) OVER w_warehouse_id_by_unit_price_r AS next_moved_at_week,
        NTILE(5) OVER (PARTITION BY warehouse_id ORDER BY unit_price_rank DESC, id) AS unit_price_rank_quintile
    FROM scored
    WINDOW w_unit_price_rank_distinct_sku AS (PARTITION BY unit_price_rank, distinct_sku_id_in_operator), w_warehouse_id_by_unit_price_r AS (PARTITION BY warehouse_id ORDER BY unit_price_rank DESC, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    operator_clean,
    moved_at_rank,
    rows_in_sku_id_move_type,
    unit_price_rank,
    prev_unit_price,
    running_qty,
    unit_price_rank_distinct_sku_id_in_operator_avg_,
    first_known_prev_unit_price,
    rows_in_sku_id_move_type_sum_6_prior,
    prev3_moved_at_week,
    moved_at_rank_vs_unit_price_rank_distinct_sku_id,
    unit_price_rank_quintile,
    CONCAT_WS('-', operator_clean, move_type) AS operator_clean_move_type_label
FROM ranked
ORDER BY rows_in_sku_id_move_type DESC
