WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.unit_price) AS prior_unit_price_12h
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.sku_id, a.moved_at
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_12h,
        s.prior_unit_price_12h,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM covered s
    JOIN skus sk ON sk.id = s.sku_id AND ARRAY_CONTAINS(sk.labels, 'express')
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM joined
    WHERE sku_unit_cost > (SELECT MIN(sku_unit_cost) FROM joined)
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_12h,
        s.prior_unit_price_12h,
        s.sku_category,
        s.sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost_2,
        sk.is_active AS sku_is_active
    FROM with_activity s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category = 'Apparel'
)
SELECT
    id,
    moved_at,
    prior_rows_12h,
    prior_unit_price_12h,
    sku_category,
    sku_unit_cost,
    sku_weight_kg,
    sku_unit_cost_2,
    sku_is_active,
    COALESCE(sku_category, 'UNKNOWN') AS sku_category_or_unknown
FROM combined_details_sku
WHERE sku_category = 'Apparel'
ORDER BY prior_rows_12h, sku_unit_cost_2 NULLS LAST
