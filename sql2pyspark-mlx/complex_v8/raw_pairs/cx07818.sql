WITH in_window AS (
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
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(moved_at) AS moved_at_year,
        ROUND(AVG(prior_unit_price_7d), 2) AS avg_prior_unit_price_7d,
        COUNT(*) AS row_count,
        MIN(moved_at) AS first_moved_at
    FROM in_window
    GROUP BY YEAR(moved_at)
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE avg_prior_unit_price_7d < (SELECT MAX(avg_prior_unit_price_7d) FROM grouped)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE row_count > (SELECT AVG(row_count) FROM screened)
)
-- Final projection
SELECT
    moved_at_year,
    avg_prior_unit_price_7d,
    row_count,
    first_moved_at,
    DATE_TRUNC('HOUR', first_moved_at) AS first_moved_at_hour
FROM with_activity
