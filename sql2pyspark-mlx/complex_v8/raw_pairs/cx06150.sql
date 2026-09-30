WITH in_window AS (
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
totals AS (
    -- Summarise per group
    SELECT
        prior_rows_7d,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        PERCENTILE_APPROX(prior_qty_7d, 0.5) AS median_prior_qty_7d,
        COUNT(*) AS cnt
    FROM in_window
    WHERE sku_id >= 16
    GROUP BY prior_rows_7d, DATE_TRUNC('MONTH', moved_at)
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT prior_rows_7d, median_prior_qty_7d
FROM totals
ORDER BY prior_rows_7d, median_prior_qty_7d
