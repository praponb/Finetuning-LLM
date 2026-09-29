WITH joined_ranges AS (
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
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        moved_at,
        prior_rows_3d,
        prior_unit_price_3d,
        COUNT(*) OVER w_prior_rows_3d AS rows_in_prior_rows_3d,
        LAG(prior_rows_3d) OVER w_sku_id_by_moved_at AS prev_prior_rows_3d,
        MIN(prior_unit_price_3d) OVER (PARTITION BY prior_rows_3d) AS prior_rows_3d_min_prior_unit_price_3d,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_prior_rows_3d_moved_at_month)) / 60, 1) AS minutes_since_prev_moved_at
    FROM joined_ranges
    WINDOW w_prior_rows_3d AS (PARTITION BY prior_rows_3d), w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at, id), w_prior_rows_3d_moved_at_month AS (PARTITION BY prior_rows_3d, DATE_TRUNC('MONTH', moved_at) ORDER BY prior_unit_price_3d NULLS LAST, id)
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        rows_in_prior_rows_3d,
        prev_prior_rows_3d,
        ROUND(AVG(prev_prior_rows_3d) OVER (ORDER BY rows_in_prior_rows_3d ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS prev_prior_rows_3d_avg_3_prior,
        prior_rows_3d - LAG(prior_rows_3d) OVER w_all_by_prev_prior_rows_3d AS prior_rows_3d_change,
        MAX(rows_in_prior_rows_3d) OVER (ORDER BY prev_prior_rows_3d DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rows_in_prior_rows_3d_max_4
    FROM windowed
    WINDOW w_all_by_prev_prior_rows_3d AS (ORDER BY prev_prior_rows_3d DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    rows_in_prior_rows_3d,
    prev_prior_rows_3d,
    prev_prior_rows_3d_avg_3_prior,
    prior_rows_3d_change,
    rows_in_prior_rows_3d_max_4,
    COALESCE(prev_prior_rows_3d, 0) AS prev_prior_rows_3d_filled
FROM windowed_2
ORDER BY rows_in_prior_rows_3d DESC, id
LIMIT 20
