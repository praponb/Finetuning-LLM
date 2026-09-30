WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.unit_price) AS prior_unit_price_7d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_7d,
        s.prior_unit_price_7d,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category
    FROM matched s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sku_id_stats AS (
    -- Max prior_unit_price_7d per sku_id
    SELECT sku_id, MAX(prior_unit_price_7d) AS max_prior_unit_price_7d
    FROM with_parent_sku
    GROUP BY sku_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_unit_price_7d,
        ROUND(s.prior_unit_price_7d - g.max_prior_unit_price_7d, 2) AS prior_unit_price_7d_minus_max
    FROM with_parent_sku s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.prior_unit_price_7d = g.max_prior_unit_price_7d
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_7d,
    prior_unit_price_7d,
    sku_code,
    sku_category,
    max_prior_unit_price_7d,
    prior_unit_price_7d_minus_max,
    DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
FROM compared
ORDER BY sku_id
