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
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(prior_rows_7d, 0) AS prior_rows_7d_filled,
        FLOOR(prior_qty_7d / 500) * 500 AS prior_qty_7d_bucket
    FROM matched
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_qty_7d,
    prior_rows_7d_filled,
    prior_qty_7d_bucket,
    ROUND(prior_rows_7d_filled / NULLIF(prior_rows_7d, 0), 3) AS prior_rows_7d_filled_per_prior_rows_7d
FROM labeled
ORDER BY moved_at NULLS LAST
