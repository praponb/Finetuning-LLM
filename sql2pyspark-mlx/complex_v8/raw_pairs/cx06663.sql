WITH recent_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM warehouses
    WHERE city = 'Singapore'
      AND opened_on BETWEEN DATE '2024-02-19' AND DATE '2025-07-03'
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.opened_on,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type,
        st.operator AS move_operator
    FROM recent_warehouses s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(move_unit_price - AVG(move_unit_price) OVER (PARTITION BY move_id), 2) AS move_unit_price_vs_move_id_avg,
        MAX(move_unit_price) OVER (PARTITION BY move_id) AS move_id_max_move_unit_price,
        SUM(move_unit_price) OVER (PARTITION BY move_type, move_id ORDER BY move_unit_price DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_unit_price,
        RANK() OVER (PARTITION BY move_id, move_operator ORDER BY move_unit_price ASC, id, move_id) AS move_unit_price_rank
    FROM line_level_moves
)
-- Subtotals with ROLLUP
SELECT
    move_id,
    move_type,
    GROUPING(move_id) AS is_total_move_id,
    SUM(CASE WHEN move_operator = 'op_bee' THEN move_unit_price ELSE 0 END) AS move_unit_price_move_operator_op_bee,
    COUNT(DISTINCT id) AS distinct_ids,
    DATEDIFF(MAX(opened_on), MIN(opened_on)) AS opened_on_span_days
FROM with_trends
GROUP BY ROLLUP(move_id, move_type)
ORDER BY move_id NULLS LAST, move_type NULLS LAST
