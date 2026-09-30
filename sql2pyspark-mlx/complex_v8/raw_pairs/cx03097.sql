WITH in_window AS (
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
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM in_window
    WHERE prior_rows_1d > (SELECT AVG(prior_rows_1d) FROM in_window)
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_1d,
    prior_unit_price_1d,
    ROUND(prior_unit_price_1d * 1.07, 2) AS prior_unit_price_1d_adj,
    DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
FROM screened
