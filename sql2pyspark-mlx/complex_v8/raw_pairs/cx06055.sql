WITH matched AS (
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
scored AS (
    -- Add calculated columns
    SELECT
        *,
        prior_unit_price_1d + prior_rows_1d AS prior_unit_price_1d_plus_prior_rows_1d
    FROM matched
)
SELECT
    id,
    moved_at,
    prior_unit_price_1d,
    prior_unit_price_1d_plus_prior_rows_1d
FROM scored
ORDER BY prior_unit_price_1d_plus_prior_rows_1d NULLS LAST
