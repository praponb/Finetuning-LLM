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
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(prior_rows_3d) IGNORE NULLS OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prior_rows_3d,
        MAX(prior_rows_3d) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prior_rows_3d_so_far,
        LEAD(prior_unit_price_3d, 1, 0) OVER (ORDER BY moved_at, id) AS next_prior_unit_price_3d,
        MIN(prior_unit_price_3d) OVER w_sku_id_prior_rows_3d AS sku_id_prior_rows_3d_min_prior_unit_price_3d,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at, id), 3) AS moved_at_percent_rank,
        ROUND(AVG(prior_rows_3d) OVER (ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_rows_3d_avg_7
    FROM joined_ranges
    WINDOW w_sku_id_prior_rows_3d AS (PARTITION BY sku_id, prior_rows_3d)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_sku_id_moved_at_month AS rows_in_sku_id_moved_at_month,
        SUM(prior_rows_3d) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), last_known_prior_rows_3d ORDER BY moved_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prior_rows_3d_sum_2,
        SUM(max_prior_rows_3d_so_far) OVER (PARTITION BY sku_id, MONTH(moved_at)) AS sku_id_moved_at_month_sum_max_prior_rows_3d_so_f
    FROM windowed
    WINDOW w_sku_id_moved_at_month AS (PARTITION BY sku_id, MONTH(moved_at))
)
-- Subtotals with ROLLUP
SELECT
    sku_id,
    prior_rows_3d,
    last_known_prior_rows_3d,
    GROUPING(sku_id) AS is_total_sku_id,
    MIN(moved_at) AS first_moved_at,
    COUNT(*) AS n_rows
FROM sequenced
GROUP BY ROLLUP(sku_id, prior_rows_3d, last_known_prior_rows_3d)
ORDER BY sku_id NULLS LAST, prior_rows_3d NULLS LAST, last_known_prior_rows_3d NULLS LAST
