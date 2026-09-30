WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.unit_price) AS prior_unit_price_3d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2,
        MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2,
        st.first_moved_at
    FROM in_window s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_unit_price_3d,
    move_count_2,
    first_moved_at,
    ROUND(prior_unit_price_3d * 1.5, 2) AS prior_unit_price_3d_adj,
    ROUND(prior_unit_price_3d * 1.1, 2) AS prior_unit_price_3d_adj_2
FROM screened
ORDER BY move_count_2
