WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.qty) AS prior_qty_12h
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.sku_id, a.moved_at
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_12h,
        sku_id,
        YEAR(moved_at) AS moved_at_year,
        MIN(moved_at) AS first_moved_at,
        MIN(prior_rows_12h) AS min_prior_rows_12h,
        PERCENTILE_APPROX(prior_qty_12h, 0.5) AS median_prior_qty_12h,
        MAX(moved_at) AS last_moved_at
    FROM joined_ranges
    GROUP BY prior_rows_12h, sku_id, YEAR(moved_at)
    HAVING AVG(prior_rows_12h) > 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT prior_rows_12h, sku_id, metric, value
FROM (
    SELECT prior_rows_12h, sku_id, min_prior_rows_12h, moved_at_year, median_prior_qty_12h
    FROM stats
) src
UNPIVOT (
    value FOR metric IN (min_prior_rows_12h, moved_at_year, median_prior_qty_12h)
) unp
