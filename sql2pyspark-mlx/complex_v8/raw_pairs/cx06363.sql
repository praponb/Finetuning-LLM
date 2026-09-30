WITH base_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        weight_kg,
        is_active
    FROM skus
    WHERE weight_kg < 21.0
      AND is_active = TRUE
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        SUM(qty) AS total_move_qty,
        COUNT(CASE WHEN operator = 'op_chan' THEN 1 END) AS move_operator_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.total_move_qty,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM base_skus s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE total_move_qty > (SELECT MIN(total_move_qty) FROM summarized_rows)
)
SELECT
    is_active,
    move_operator_count,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    ROUND(STDDEV(move_operator_count), 2) AS stddev_move_operator_count
FROM with_activity
GROUP BY ROLLUP(is_active, move_operator_count)
