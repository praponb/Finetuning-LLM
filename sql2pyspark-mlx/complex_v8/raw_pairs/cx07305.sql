WITH recent AS (
    -- Filter warehouses
    SELECT *
    FROM warehouses
    WHERE city NOT IN ('Bangkok', 'Kuala Lumpur')
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'ADJUST') FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_move_type_count,
        (SELECT COUNT(CASE WHEN st.operator IN ('op_dew', 'op_ann') THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_operator_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count
    FROM recent s
),
totals AS (
    -- Summarise per group
    SELECT
        move_operator_count,
        ROUND(SUM(move_operator_count) / NULLIF(SUM(move_count), 0), 3) AS move_operator_count_to_move_count_ratio,
        SUM(move_operator_count) FILTER (WHERE LENGTH(TRIM(wh_code)) >= 7) AS move_operator_count_length_trim_wh_code_7
    FROM profiled
    WHERE capacity >= 4912
    GROUP BY move_operator_count
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE move_operator_count_length_trim_wh_code_7 < (SELECT MAX(move_operator_count_length_trim_wh_code_7) FROM totals)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(move_operator_count) OVER (PARTITION BY move_operator_count) AS move_operator_count_max_move_operator_count,
        NTH_VALUE(move_operator_count, 2) OVER (PARTITION BY move_operator_count ORDER BY move_operator_count_length_trim_wh_code_7, move_operator_count ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_operator_count,
        ROUND(move_operator_count / NULLIF(SUM(move_operator_count) OVER (PARTITION BY move_operator_count), 0), 4) AS move_operator_count_share_of_move_operator_count
    FROM qualified
    WINDOW w_move_operator_count AS (PARTITION BY move_operator_count)
)
SELECT
    move_operator_count,
    move_operator_count_to_move_count_ratio,
    move_operator_count_length_trim_wh_code_7,
    move_operator_count_max_move_operator_count,
    second_move_operator_count
FROM with_windows
WHERE move_operator_count BETWEEN 2 AND 7
ORDER BY move_operator_count_length_trim_wh_code_7 DESC NULLS FIRST
