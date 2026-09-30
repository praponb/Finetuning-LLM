WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.qty) AS prior_qty_7d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_qty_7d,
        sk.unit_cost AS sku_unit_cost
    FROM joined_ranges s
    JOIN skus sk ON sk.id = s.sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM enriched s
)
SELECT id, moved_at, moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d
FROM profiled
ORDER BY id
LIMIT 5
