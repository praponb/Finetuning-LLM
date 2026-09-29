WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        SUBSTRING(TRIM(wh_code), 1, 3) AS wh_code_prefix
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.id) AS min_move_unit_price,
        (SELECT MAX(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.id) AS max_move_unit_price,
        (SELECT COUNT(CASE WHEN st.operator <> 'op_dew' THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_operator_count
    FROM prepared s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        wh_code_prefix,
        min_move_unit_price,
        max_move_unit_price,
        SUM(capacity) OVER w_opened_on_year AS opened_on_year_sum_capacity,
        COUNT(*) OVER (PARTITION BY move_operator_count, city ORDER BY opened_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(move_operator_count - AVG(move_operator_count) OVER (PARTITION BY YEAR(opened_on)), 2) AS move_operator_count_vs_opened_on_year_avg,
        LAG(capacity) OVER w_move_operator_count_city_by_2 AS prev_capacity,
        SUM(move_operator_count) OVER w_opened_on_year AS opened_on_year_sum_move_operator_count
    FROM with_counts
    WINDOW w_opened_on_year AS (PARTITION BY YEAR(opened_on)), w_move_operator_count_city_by_2 AS (PARTITION BY move_operator_count, city ORDER BY opened_on DESC NULLS FIRST, id)
)
SELECT
    id,
    wh_code,
    city,
    wh_code_prefix,
    min_move_unit_price,
    max_move_unit_price,
    opened_on_year_sum_capacity,
    running_count,
    prev_capacity,
    opened_on_year_sum_move_operator_count,
    ADD_MONTHS(opened_on, 6) AS opened_on_plus_6m,
    DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter
FROM windowed
WHERE move_operator_count_vs_opened_on_year_avg <= 1.6
ORDER BY wh_code DESC
