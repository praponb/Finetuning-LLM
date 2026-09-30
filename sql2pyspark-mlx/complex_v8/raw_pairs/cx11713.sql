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
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
    WHERE (moved_at < TIMESTAMP '2025-03-08 11:00:00' OR unit_price BETWEEN 37.6 AND 341.5)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        move_type_prefix,
        moved_at_week,
        MIN(qty) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_min_qty,
        LEAD(moved_at) OVER w_all_by_qty AS next_moved_at,
        COUNT(*) OVER (PARTITION BY warehouse_id, operator) AS rows_in_warehouse_id_operator,
        LAG(qty) OVER (PARTITION BY TO_DATE(moved_at_week), move_type ORDER BY moved_at_week DESC, id) AS prev_qty,
        RANK() OVER (PARTITION BY TO_DATE(moved_at_week), move_type ORDER BY moved_at_week DESC) AS moved_at_week_rank,
        MAX(qty) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_max_qty,
        NTH_VALUE(moved_at_week, 2) OVER (PARTITION BY move_type, MONTH(moved_at_week) ORDER BY moved_at_week DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_week,
        LAG(moved_at_week) OVER w_moved_at_week_to_date_move_t AS prev_moved_at_week,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty DESC), 3) AS qty_percent_rank,
        LAST_VALUE(qty) OVER (PARTITION BY YEAR(moved_at_week), operator ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        RANK() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS moved_at_rank
    FROM recent_moves
    WINDOW w_all_by_qty AS (ORDER BY qty DESC, id), w_moved_at_week_to_date_move_t AS (PARTITION BY TO_DATE(moved_at_week), move_type ORDER BY moved_at_week DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        move_type_prefix,
        moved_at_week,
        warehouse_id_operator_min_qty,
        next_moved_at,
        rows_in_warehouse_id_operator,
        prev_qty,
        moved_at_week_rank,
        warehouse_id_operator_max_qty,
        second_moved_at_week,
        prev_moved_at_week,
        qty_percent_rank,
        last_qty
    FROM metrics
    WHERE moved_at_rank = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(moved_at_week_rank) OVER w_sku_id_next_moved_at_month AS sku_id_next_moved_at_month_max_moved_at_week_ran,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY last_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND(AVG(qty_percent_rank) OVER (PARTITION BY YEAR(next_moved_at) ORDER BY prev_qty DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_percent_rank_avg_2,
        ROUND(moved_at_week_rank / NULLIF(SUM(moved_at_week_rank) OVER (PARTITION BY sku_id, MONTH(next_moved_at)), 0), 4) AS moved_at_week_rank_share_of_sku_id_next_moved_at
    FROM leaders
    WINDOW w_sku_id_next_moved_at_month AS (PARTITION BY sku_id, MONTH(next_moved_at))
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    move_type_prefix,
    warehouse_id_operator_min_qty,
    rows_in_warehouse_id_operator,
    moved_at_week_rank,
    warehouse_id_operator_max_qty,
    prev_moved_at_week,
    qty_percent_rank,
    last_qty,
    second_moved_at,
    qty_percent_rank_avg_2,
    moved_at_week_rank_share_of_sku_id_next_moved_at
FROM ranked
WHERE operator IN ('op_bee', 'op_eve')
ORDER BY move_type DESC, id
LIMIT 20
