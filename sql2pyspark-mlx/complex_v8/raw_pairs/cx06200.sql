WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE (warehouse_id >= 4 OR sku_id <= 14)
),
grouped AS (
    -- Summarise per group
    SELECT
        move_type_len,
        SUM(CASE WHEN operator = 'op_chan' THEN 1 ELSE 0 END) AS n_operator_op_chan,
        COUNT(*) AS cnt,
        SUM(unit_price) AS total_unit_price,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM prepared_moves
    WHERE move_type_prefix IS NOT NULL
    GROUP BY 1
    HAVING SUM(move_type_len) > 7
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE cnt < (SELECT MAX(cnt) FROM grouped)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        move_type_len,
        n_operator_op_chan,
        cnt,
        ROUND(move_type_len / NULLIF(SUM(move_type_len) OVER (PARTITION BY move_type_len, cnt), 0), 4) AS move_type_len_share_of_move_type_len_cnt,
        SUM(cnt) OVER (PARTITION BY move_type_len, cnt) AS move_type_len_cnt_sum_cnt,
        MAX(total_unit_price) OVER (ORDER BY cnt DESC NULLS FIRST, move_type_len ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS total_unit_price_max_4,
        LEAD(total_unit_price) OVER w_all_by_cnt AS next_total_unit_price
    FROM without_matches
    WINDOW w_all_by_cnt AS (ORDER BY cnt DESC NULLS FIRST, move_type_len)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        move_type_len,
        n_operator_op_chan,
        cnt,
        move_type_len_cnt_sum_cnt,
        total_unit_price_max_4,
        next_total_unit_price,
        FIRST_VALUE(next_total_unit_price) IGNORE NULLS OVER (PARTITION BY n_operator_op_chan ORDER BY move_type_len_share_of_move_type_len_cnt, move_type_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_total_unit_price,
        n_operator_op_chan - LAG(n_operator_op_chan) OVER (PARTITION BY n_operator_op_chan ORDER BY move_type_len_share_of_move_type_len_cnt, move_type_len) AS n_operator_op_chan_change,
        LEAD(move_type_len) OVER (PARTITION BY n_operator_op_chan ORDER BY move_type_len_share_of_move_type_len_cnt, move_type_len) AS next_move_type_len
    FROM with_windows
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        cnt - LAG(cnt) OVER w_move_type_len_by_next_total AS cnt_change,
        ROUND(CUME_DIST() OVER w_move_type_len_by_next_total, 3) AS next_total_unit_price_cume_dist,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY move_type_len, cnt), 0), 4) AS cnt_share_of_move_type_len_cnt,
        FIRST_VALUE(total_unit_price_max_4) OVER (PARTITION BY move_type_len ORDER BY next_total_unit_price DESC NULLS FIRST, move_type_len ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_unit_price_max_4,
        ROW_NUMBER() OVER (PARTITION BY move_type_len ORDER BY next_total_unit_price DESC NULLS FIRST, move_type_len) AS rn
    FROM with_trends
    WINDOW w_move_type_len_by_next_total AS (PARTITION BY move_type_len ORDER BY next_total_unit_price DESC NULLS FIRST, move_type_len)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM with_trends_2
    WHERE rn <= 5
)
SELECT
    move_type_len,
    n_operator_op_chan,
    cnt,
    move_type_len_cnt_sum_cnt,
    total_unit_price_max_4,
    next_total_unit_price,
    first_known_next_total_unit_price,
    n_operator_op_chan_change,
    next_move_type_len,
    cnt_change,
    next_total_unit_price_cume_dist,
    cnt_share_of_move_type_len_cnt,
    rn,
    GREATEST(next_move_type_len, next_total_unit_price) AS greatest_next_move_type_len_next_total_unit_pric
FROM top_per_group
ORDER BY n_operator_op_chan_change
