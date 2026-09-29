WITH matched AS (
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
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        prior_rows_12h,
        prior_qty_12h + prior_rows_12h AS prior_qty_12h_plus_prior_rows_12h,
        ABS(prior_rows_12h - 4) AS prior_rows_12h_distance,
        COALESCE(prior_qty_12h, 0) AS prior_qty_12h_filled
    FROM matched
    WHERE prior_qty_12h >= 1125
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE prior_qty_12h_plus_prior_rows_12h > (SELECT AVG(prior_qty_12h_plus_prior_rows_12h) FROM enriched)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(prior_rows_12h / NULLIF(prior_qty_12h_filled, 0), 3) AS prior_rows_12h_per_prior_qty_12h_filled
    FROM without_matches
    WHERE prior_rows_12h > 2
)
-- Final projection
SELECT
    id,
    prior_rows_12h_distance,
    prior_qty_12h_filled,
    prior_rows_12h_per_prior_qty_12h_filled,
    CASE WHEN prior_rows_12h_distance >= 5 THEN 'large' WHEN prior_rows_12h_distance >= 3 THEN 'medium' ELSE 'small' END AS prior_rows_12h_distance_tier
FROM annotated
