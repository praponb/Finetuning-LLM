WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        COUNT(*) FILTER (WHERE operator IN ('op_bee', 'op_chan', 'op_eve')) AS n_operator_in_op_bee_op_chan_o,
        SUM(CASE WHEN operator IS NOT NULL THEN unit_price ELSE 0 END) AS unit_price_operator_is_not_null,
        MAX(moved_at) AS last_moved_at,
        MIN(unit_price) AS min_unit_price,
        SUM(CASE WHEN move_type = 'ADJUST' THEN 1 ELSE 0 END) AS n_move_type_adjust,
        COUNT(*) AS cnt
    FROM prepared_moves
    GROUP BY 1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN cnt >= 10 THEN 'HIGH' WHEN cnt >= 6 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier,
        YEAR(moved_at_day) AS moved_at_day_year
    FROM grouped
    WHERE min_unit_price BETWEEN 25.9 AND 450.1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(moved_at_day) OVER (PARTITION BY cnt_tier ORDER BY n_move_type_adjust DESC, moved_at_day) AS prev_moved_at_day,
        DENSE_RANK() OVER (PARTITION BY cnt_tier ORDER BY n_move_type_adjust DESC) AS n_move_type_adjust_dense_rank,
        DATEDIFF(moved_at_day, LAG(moved_at_day) OVER (PARTITION BY cnt_tier ORDER BY n_move_type_adjust DESC, moved_at_day)) AS days_since_prev_moved_at_day,
        MIN(moved_at_day_year) OVER w_n_operator_in_op_bee_op_chan AS n_operator_in_op_bee_op_chan_o_moved_at_day_year,
        SUM(moved_at_day_year) OVER (PARTITION BY n_operator_in_op_bee_op_chan_o, moved_at_day_year) AS n_operator_in_op_bee_op_chan_o_moved_at_day_year_2
    FROM annotated
    WINDOW w_n_operator_in_op_bee_op_chan AS (PARTITION BY n_operator_in_op_bee_op_chan_o, moved_at_day_year)
)
SELECT
    cnt_tier,
    moved_at_day_year,
    SUM(n_operator_in_op_bee_op_chan_o) AS total_n_operator_in_op_bee_op_chan_o,
    COUNT(*) AS row_count,
    COUNT(DISTINCT cnt_tier) AS distinct_cnt_tiers
FROM metrics
GROUP BY GROUPING SETS ((cnt_tier, moved_at_day_year), (cnt_tier), (moved_at_day_year))
