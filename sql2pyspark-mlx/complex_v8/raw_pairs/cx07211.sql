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
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_1d,
        sk.unit_cost AS sku_unit_cost
    FROM covered s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    moved_at,
    prior_rows_1d,
    sku_unit_cost,
    DATE_TRUNC('DAY', moved_at) AS moved_at_day
FROM joined_sku
ORDER BY moved_at
