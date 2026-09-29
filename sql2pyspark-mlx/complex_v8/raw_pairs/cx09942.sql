WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.unit_price) AS prior_unit_price_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        sku_id,
        MIN(prior_unit_price_1d) AS min_prior_unit_price_1d,
        MAX(moved_at) AS last_moved_at
    FROM covered
    GROUP BY DATE_TRUNC('MONTH', moved_at), sku_id
    HAVING MAX(prior_unit_price_1d) > 500.0
)
SELECT moved_at_month, min_prior_unit_price_1d, last_moved_at
FROM per_group
ORDER BY last_moved_at NULLS LAST, moved_at_month NULLS LAST
