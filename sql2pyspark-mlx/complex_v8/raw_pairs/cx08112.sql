WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.qty) AS prior_qty_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
sku_id_stats AS (
    -- Median prior_qty_1d per sku_id
    SELECT sku_id, PERCENTILE_APPROX(prior_qty_1d, 0.5) AS median_prior_qty_1d
    FROM joined_ranges
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_qty_1d,
        ROUND(s.prior_qty_1d - g.median_prior_qty_1d, 2) AS prior_qty_1d_minus_median
    FROM joined_ranges s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.prior_qty_1d >= g.median_prior_qty_1d
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_1d,
    median_prior_qty_1d,
    ROUND(median_prior_qty_1d / NULLIF(prior_qty_1d, 0), 3) AS median_prior_qty_1d_per_prior_qty_1d,
    prior_qty_1d + prior_rows_1d AS prior_qty_1d_plus_prior_rows_1d
FROM vs_group
ORDER BY median_prior_qty_1d, id
LIMIT 5
