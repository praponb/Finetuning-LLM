WITH recent_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE opened_on < DATE '2025-07-03'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY city, DATE_TRUNC('MONTH', opened_on)) AS rows_in_city_opened_on_month,
        LEAD(capacity) OVER w_opened_on_month_city_by_capa AS next_capacity
    FROM recent_warehouses
    WINDOW w_opened_on_month_city_by_capa AS (PARTITION BY MONTH(opened_on), city ORDER BY capacity, id)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator <> 'op_eve') FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_operator_count
    FROM with_trends s
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'INBOUND') FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_move_type_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.id) AS max_move_qty
    FROM with_stats s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(move_operator_count) OVER (ORDER BY move_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_move_operator_count_so_far,
        MAX(max_move_qty) OVER (PARTITION BY move_count) AS move_count_max_max_move_qty,
        COUNT(*) OVER (PARTITION BY move_count) AS rows_in_move_count,
        SUM(move_move_type_count) OVER (PARTITION BY move_count) AS move_count_sum_move_move_type_count,
        DENSE_RANK() OVER (ORDER BY move_count) AS move_count_dense_rank
    FROM summarized_rows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, move_count, next_capacity
    FROM sequenced
) src
UNPIVOT (
    amount FOR field IN (move_count, next_capacity)
) unp
