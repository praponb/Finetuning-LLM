WITH covered AS (
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
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_1d,
        s.prior_qty_1d,
        sk.category AS sku_category
    FROM covered s
    JOIN skus sk ON sk.id = s.sku_id
),
sku_category_stats AS (
    -- Max prior_qty_1d per sku_category
    SELECT sku_category, MAX(prior_qty_1d) AS max_prior_qty_1d
    FROM joined_sku
    GROUP BY sku_category
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_qty_1d,
        ROUND(s.prior_qty_1d - g.max_prior_qty_1d, 2) AS prior_qty_1d_minus_max
    FROM joined_sku s
    JOIN sku_category_stats g ON g.sku_category = s.sku_category
    WHERE s.prior_qty_1d = g.max_prior_qty_1d
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_1d,
    prior_qty_1d,
    sku_category,
    max_prior_qty_1d,
    prior_qty_1d_minus_max,
    SUBSTRING(TRIM(sku_category), 1, 4) AS sku_category_prefix
FROM outliers
WHERE prior_qty_1d BETWEEN 375 AND 2125
ORDER BY max_prior_qty_1d, moved_at
