WITH matched AS (
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
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at_2
    FROM matched
)
-- Final projection
SELECT
    id,
    moved_at,
    prior_rows_7d,
    days_since_moved_at_2,
    COALESCE(prior_rows_7d, 0) AS prior_rows_7d_filled,
    days_since_moved_at + prior_qty_7d AS days_since_moved_at_plus_prior_qty_7d
FROM annotated
WHERE moved_at < TIMESTAMP '2025-03-02 09:00:00'
ORDER BY prior_rows_7d_filled
