WITH joined_ranges AS (
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
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        prior_rows_1d,
        ROUND(prior_unit_price_1d - AVG(prior_unit_price_1d) OVER w_sku_id_prior_rows_1d, 2) AS prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY prior_rows_1d DESC), 3) AS prior_rows_1d_cume_dist,
        ROUND(prior_rows_1d / NULLIF(SUM(prior_rows_1d) OVER (PARTITION BY sku_id, prior_rows_1d), 0), 4) AS prior_rows_1d_share_of_sku_id_prior_rows_1d,
        ROUND(PERCENT_RANK() OVER w_sku_id_by_prior_rows_1d, 3) AS prior_rows_1d_percent_rank,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY prior_rows_1d DESC) AS prior_rows_1d_dense_rank
    FROM joined_ranges
    WINDOW w_sku_id_prior_rows_1d AS (PARTITION BY sku_id, prior_rows_1d), w_sku_id_by_prior_rows_1d AS (PARTITION BY sku_id ORDER BY prior_rows_1d DESC, id)
),
with_parent AS (
    -- Attach sku attributes
    SELECT s.*, sk.is_active AS sku_is_active, sk.sku_code
    FROM windowed s
    JOIN skus sk ON sk.id = s.sku_id
),
totals AS (
    -- Summarise per group
    SELECT
        sku_id,
        prior_rows_1d,
        ROUND(SUM(prior_rows_1d_cume_dist), 2) AS total_prior_rows_1d_cume_dist,
        ROUND(AVG(prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg), 2) AS avg_prior_unit_price_1d_vs_sku_id_prior_rows_1d_,
        COUNT(*) FILTER (WHERE UPPER(TRIM(sku_code)) LIKE 'E%') AS n_upper_trim_sku_code_like_e,
        ROUND(SUM(prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg) / NULLIF(SUM(prior_rows_1d_cume_dist), 0), 3) AS prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg_,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE sku_is_active = TRUE) AS n_sku_is_active_true
    FROM with_parent
    GROUP BY sku_id, prior_rows_1d
    HAVING MAX(prior_rows_1d_percent_rank) > 0.1
)
-- Final projection
SELECT
    sku_id,
    prior_rows_1d,
    n_upper_trim_sku_code_like_e,
    prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg_,
    row_count,
    CASE WHEN n_sku_is_active_true >= 8 THEN 'HIGH' WHEN n_sku_is_active_true >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS n_sku_is_active_true_tier
FROM totals
WHERE n_upper_trim_sku_code_like_e <= 6
ORDER BY sku_id
