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
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY sku_id, prior_rows_1d) AS rows_in_sku_id_prior_rows_1d,
        ROUND(prior_rows_1d - AVG(prior_rows_1d) OVER (PARTITION BY sku_id, prior_rows_1d), 2) AS prior_rows_1d_vs_sku_id_prior_rows_1d_avg,
        ROUND(AVG(prior_rows_1d) OVER (PARTITION BY TO_DATE(moved_at) ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_rows_1d_avg_7
    FROM in_window
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prior_unit_price_1d >= 2000.0 THEN 'large' WHEN prior_unit_price_1d >= 1000.0 THEN 'medium' ELSE 'small' END AS prior_unit_price_1d_tier,
        COUNT(*) AS cnt,
        ROUND(SUM(prior_rows_1d_avg_7) / NULLIF(SUM(rows_in_sku_id_prior_rows_1d), 0), 3) AS prior_rows_1d_avg_7_to_rows_in_sku_id_prior_rows
    FROM windowed
    GROUP BY CASE WHEN prior_unit_price_1d >= 2000.0 THEN 'large' WHEN prior_unit_price_1d >= 1000.0 THEN 'medium' ELSE 'small' END
)
SELECT
    prior_unit_price_1d_tier,
    cnt,
    prior_rows_1d_avg_7_to_rows_in_sku_id_prior_rows,
    FLOOR(cnt / 2) * 2 AS cnt_bucket
FROM stats
ORDER BY prior_rows_1d_avg_7_to_rows_in_sku_id_prior_rows DESC, cnt DESC
