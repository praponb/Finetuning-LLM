WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM cleaned
    WHERE unit_price < (SELECT MAX(unit_price) FROM cleaned)
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        st.id AS move_id,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM without_matches s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.operator IN ('op_ann', 'op_chan', 'op_eve')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_id,
        move_operator,
        move_qty,
        move_moved_at,
        DENSE_RANK() OVER (PARTITION BY move_id, sku_id ORDER BY move_moved_at) AS move_moved_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(move_moved_at) ORDER BY move_moved_at ASC) AS move_moved_at_dense_rank_2
    FROM line_level
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_moved_at, move_moved_at_dense_rank_2, move_operator, move_moved_at_dense_rank
    FROM metrics
) src
PIVOT (
    SUM(move_moved_at_dense_rank)
    FOR move_operator IN ('op_bee', 'op_dew', 'op_eve', 'op_chan')
)
ORDER BY move_moved_at, move_moved_at_dense_rank_2
