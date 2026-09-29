WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'TRANSFER')
      AND warehouse_id >= 5
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
        unit_price,
        COUNT(*) OVER (PARTITION BY operator, move_type) AS rows_in_operator_move_type,
        SIZE(COLLECT_SET(warehouse_id) OVER w_operator_move_type) AS distinct_warehouse_id_in_operator_move_type,
        NTH_VALUE(qty, 2) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM prepared
    WINDOW w_operator_move_type AS (PARTITION BY operator, move_type)
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IS NOT NULL
        )
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        moved_at,
        distinct_warehouse_id_in_operator_move_type,
        COUNT(*) OVER w_operator_warehouse_id AS rows_in_operator_warehouse_id,
        ROW_NUMBER() OVER (ORDER BY rows_in_operator_move_type, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (ORDER BY rows_in_operator_move_type, id))) / 60, 1) AS minutes_since_prev_moved_at,
        LAG(qty, 3) OVER (ORDER BY rows_in_operator_move_type, id) AS prev3_qty,
        NTILE(4) OVER (ORDER BY second_qty DESC, id) AS second_qty_quartile,
        SUM(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_sum_7
    FROM without_matches
    WINDOW w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_warehouse_id_in_operator_move_type,
        second_qty_quartile,
        TO_DATE(moved_at) AS moved_at_day,
        ROUND(AVG(distinct_warehouse_id_in_operator_move_type), 2) AS avg_distinct_warehouse_id_in_operator_move_type,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(moved_at) AS last_moved_at
    FROM with_trends
    GROUP BY distinct_warehouse_id_in_operator_move_type, second_qty_quartile, TO_DATE(moved_at)
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(distinct_ids) OVER (PARTITION BY second_qty_quartile) AS second_qty_quartile_max_distinct_ids,
        COUNT(*) OVER (PARTITION BY second_qty_quartile) AS rows_in_second_qty_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY second_qty_quartile ORDER BY distinct_ids), 3) AS distinct_ids_percent_rank
    FROM rollup_base
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        distinct_warehouse_id_in_operator_move_type,
        second_qty_quartile,
        moved_at_day,
        distinct_ids,
        distinct_ids_percent_rank,
        FIRST_VALUE(moved_at_day) IGNORE NULLS OVER (ORDER BY second_qty_quartile_max_distinct_ids DESC, distinct_warehouse_id_in_operator_move_type, second_qty_quartile, moved_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_day,
        ROUND(AVG(distinct_ids) OVER (ORDER BY second_qty_quartile_max_distinct_ids DESC, distinct_warehouse_id_in_operator_move_type, second_qty_quartile, moved_at_day ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS distinct_ids_avg_6_prior
    FROM with_trends_2
)
-- Final projection
SELECT
    distinct_warehouse_id_in_operator_move_type,
    second_qty_quartile,
    distinct_ids_percent_rank,
    first_known_moved_at_day,
    distinct_ids_avg_6_prior,
    ROUND(distinct_ids_avg_6_prior * 1.1, 2) AS distinct_ids_avg_6_prior_adj
FROM metrics_2
