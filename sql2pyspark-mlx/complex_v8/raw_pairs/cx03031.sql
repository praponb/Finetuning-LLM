WITH joined_ranges AS (
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
    SELECT sku_id, COUNT(*) AS move_count, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.first_moved_at
    FROM joined_ranges s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        moved_at,
        prior_rows_3d,
        prior_unit_price_3d,
        first_moved_at,
        DATEDIFF(DATE '2025-06-30', first_moved_at) AS days_since_first_moved_at
    FROM summarized_rows
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE prior_rows_3d > (SELECT MIN(prior_rows_3d) FROM scored)
)
SELECT id, prior_rows_3d, days_since_first_moved_at
FROM with_activity
ORDER BY id, prior_rows_3d DESC
