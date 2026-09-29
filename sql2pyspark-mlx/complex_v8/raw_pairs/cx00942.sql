WITH matched AS (
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
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        moved_at,
        prior_unit_price_12h,
        SIZE(COLLECT_SET(sku_id) OVER w_prior_rows_12h) AS distinct_sku_id_in_prior_rows_12h,
        LAG(prior_rows_12h) OVER w_prior_rows_12h_by_moved_at AS prev_prior_rows_12h
    FROM matched
    WINDOW w_prior_rows_12h AS (PARTITION BY prior_rows_12h), w_prior_rows_12h_by_moved_at AS (PARTITION BY prior_rows_12h ORDER BY moved_at, id)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE prior_unit_price_12h < (SELECT MAX(prior_unit_price_12h) FROM metrics)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        COALESCE(prev_prior_rows_12h, 0) AS prev_prior_rows_12h_filled
    FROM without_matches
    WHERE moved_at < TIMESTAMP '2025-03-02 12:00:00'
)
-- Final projection
SELECT
    id,
    moved_at,
    distinct_sku_id_in_prior_rows_12h,
    prev_prior_rows_12h,
    prev_prior_rows_12h_filled,
    LEAST(prior_unit_price_12h, prev_prior_rows_12h) AS least_prior_unit_price_12h_prev_prior_rows_12h,
    DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
FROM tagged
