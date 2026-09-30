WITH covered AS (
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
sku_id_stats AS (
    -- Median prior_unit_price_3d per sku_id
    SELECT sku_id, PERCENTILE_APPROX(prior_unit_price_3d, 0.5) AS median_prior_unit_price_3d
    FROM covered
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_unit_price_3d,
        ROUND(s.prior_unit_price_3d - g.median_prior_unit_price_3d, 2) AS prior_unit_price_3d_minus_median
    FROM covered s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.prior_unit_price_3d >= g.median_prior_unit_price_3d
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_3d,
    prior_unit_price_3d,
    DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
FROM vs_group
