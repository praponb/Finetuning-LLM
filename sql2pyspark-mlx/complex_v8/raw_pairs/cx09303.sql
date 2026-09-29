WITH in_window AS (
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
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        MIN(moved_at) AS first_moved_at
    FROM in_window
    GROUP BY sku_id
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.sku_id,
        s.distinct_sku_ids,
        sk.unit_cost AS sku_unit_cost,
        sk.sku_code
    FROM grouped s
    JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost >= 108.5
)
-- Final projection
SELECT
    sku_id,
    distinct_sku_ids,
    sku_unit_cost,
    sku_code
FROM with_parent_sku
WHERE sku_unit_cost > 60.9
ORDER BY distinct_sku_ids, sku_unit_cost DESC NULLS FIRST
