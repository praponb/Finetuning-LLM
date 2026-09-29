WITH base AS (
    SELECT
        *,
        DATE_SUB(ends_on, 32) AS ends_on_minus_32d,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE channel IS DISTINCT FROM 'mobile'
),
filtered AS (
    SELECT *, CASE WHEN promo_code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5
    FROM base
    WHERE ends_on > DATE '2023-08-11'
      AND channel IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        ends_on,
        discount_pct,
        ends_on_minus_32d,
        channel_clean,
        is_welcome5,
        AVG(is_welcome5) OVER w_recent AS is_welcome5_avg_2,
        AVG(is_welcome5) OVER w_ordered AS is_welcome5_avg_2_2,
        FIRST_VALUE(channel) IGNORE NULLS OVER w_running AS first_known_channel
    FROM filtered
    WINDOW w_recent AS (ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY DATE_TRUNC('MONTH', starts_on) ORDER BY is_welcome5, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY promo_code, channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(is_welcome5_avg_2_2, TRUE) OVER (ORDER BY is_welcome5_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_welcome5_avg_2_2,
        RANK() OVER (ORDER BY discount_pct DESC) AS discount_pct_rank,
        NTILE(3) OVER (ORDER BY is_welcome5_avg_2_2, id) AS is_welcome5_avg_2_2_bucket,
        AVG(is_welcome5) OVER (ORDER BY discount_pct, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_welcome5_avg_5_prior
    FROM with_windows
    WHERE is_welcome5_avg_2 BETWEEN 23 AND 56
),
filtered_promotions AS (
    SELECT *, LEAST(is_welcome5_avg_5_prior, discount_pct) AS least_is_welcome5_avg_5_prior_discount_pct
    FROM metrics
    WHERE is_welcome5 BETWEEN 39 AND 78
),
valid AS (
    SELECT
        id,
        ends_on,
        ends_on_minus_32d,
        channel_clean,
        is_welcome5_avg_2,
        is_welcome5_avg_2_2,
        last_known_is_welcome5_avg_2_2,
        discount_pct_rank,
        is_welcome5_avg_2_2_bucket,
        is_welcome5_avg_5_prior,
        last_known_is_welcome5_avg_2_2 * is_welcome5_avg_2_2_bucket AS last_known_is_welcome5_avg_2_2_times_is_welcome5_avg_2_2_bucket
    FROM filtered_promotions
    WHERE last_known_is_welcome5_avg_2_2 BETWEEN 41 AND 75
)
SELECT
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    CASE WHEN discount_pct_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_rank_tier,
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    COUNT(*) AS num_rows,
    ROUND(AVG(last_known_is_welcome5_avg_2_2), 2) AS avg_last_known_is_welcome5_avg_2_2,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(last_known_is_welcome5_avg_2_2_times_is_welcome5_avg_2_2_bucket) AS total_last_known_is_welcome5_avg_2_2_times_is_welcome5_avg_2_2_bucket,
    GROUPING_ID() AS gid
FROM valid
GROUP BY CUBE (DATE_TRUNC('YEAR', ends_on), CASE WHEN discount_pct_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', ends_on))
