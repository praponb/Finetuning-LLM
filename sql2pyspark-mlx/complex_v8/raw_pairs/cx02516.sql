WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.qty) AS prior_qty_3d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.prior_rows_3d,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM joined_ranges s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.unit_price < 289.7
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY move_unit_price, id, move_id) AS seq_no,
        COUNT(*) OVER (PARTITION BY move_id) AS rows_in_move_id,
        COUNT(*) OVER (ORDER BY move_unit_price, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_children
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.move_type NOT IN ('TRANSFER', 'INBOUND') THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_move_type_count,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS first_moved_at_2
    FROM with_windows s
)
SELECT *
FROM (
    SELECT first_moved_at, sku_id, move_operator, rows_in_move_id
    FROM with_counts
) src
PIVOT (
    COUNT(rows_in_move_id) AS count_v, SUM(rows_in_move_id) AS sum_v
    FOR move_operator IN ('op_dew', 'op_ann', 'op_eve')
)
