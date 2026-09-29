WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        SUM(discount_pct) OVER (PARTITION BY MONTH(ends_on), promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        LAG(discount_pct, 1) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY YEAR(ends_on), channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct
    FROM promotions
    WHERE promo_code NOT IN ('VIP', 'SPRING10')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        cumulative_discount_pct,
        prev_discount_pct,
        DENSE_RANK() OVER (PARTITION BY promo_code, channel ORDER BY running_discount_pct NULLS LAST) AS running_discount_pct_dense_rank,
        ROUND(cumulative_discount_pct / SUM(cumulative_discount_pct) OVER (PARTITION BY channel), 4) AS cumulative_discount_pct_share,
        ROUND(prev_discount_pct / SUM(prev_discount_pct) OVER (PARTITION BY channel), 4) AS prev_discount_pct_share,
        LAST_VALUE(cumulative_discount_pct) IGNORE NULLS OVER (PARTITION BY channel ORDER BY prev_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_discount_pct,
        LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY cumulative_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        cumulative_discount_pct,
        NTILE(5) OVER (ORDER BY prev_discount_pct DESC, id) AS prev_discount_pct_bucket,
        PERCENT_RANK() OVER (ORDER BY cumulative_discount_pct_share DESC NULLS FIRST) AS cumulative_discount_pct_share_percent_rank
    FROM metrics
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        cumulative_discount_pct * prev_discount_pct_bucket AS cumulative_discount_pct_times_prev_discount_pct_bucket,
        cumulative_discount_pct - prev_discount_pct_bucket AS cumulative_discount_pct_minus_prev_discount_pct_bucket
    FROM scored
    WHERE prev_discount_pct_bucket IS NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN cumulative_discount_pct_times_prev_discount_pct_bucket >= 86 THEN 'High' WHEN cumulative_discount_pct_times_prev_discount_pct_bucket >= 26 THEN 'Medium' ELSE 'Low' END AS cumulative_discount_pct_times_prev_discount_pct_bucket_level
    FROM base
    WHERE prev_discount_pct_bucket > 4
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(cumulative_discount_pct, 0) AS cumulative_discount_pct_rounded
    FROM cleaned
    WHERE cumulative_discount_pct_minus_prev_discount_pct_bucket > 78
),
with_windows_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cumulative_discount_pct,
        cumulative_discount_pct_share_percent_rank,
        cumulative_discount_pct_times_prev_discount_pct_bucket,
        cumulative_discount_pct_minus_prev_discount_pct_bucket,
        AVG(cumulative_discount_pct) OVER w_hist AS cumulative_discount_pct_avg_6_prior,
        LEAD(prev_discount_pct_bucket, 2, 0) OVER w_hist_2 AS next_prev_discount_pct_bucket
    FROM filtered
    WINDOW w_hist AS (ORDER BY cumulative_discount_pct_times_prev_discount_pct_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_hist_2 AS (ORDER BY prev_discount_pct_bucket, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_discount_pct_times_prev_discount_pct_bucket >= 90 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_discount_pct_times_prev_discount_pct_bucket_tier,
    CASE WHEN cumulative_discount_pct_share_percent_rank >= 0.82 THEN 'HIGH' ELSE 'LOW' END AS cumulative_discount_pct_share_percent_rank_tier,
    ROUND(AVG(next_prev_discount_pct_bucket), 2) AS avg_next_prev_discount_pct_bucket,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(cumulative_discount_pct_avg_6_prior, 0.9) AS cumulative_discount_pct_avg_6_prior_p90,
    PERCENTILE_APPROX(cumulative_discount_pct, 0.95) AS cumulative_discount_pct_p95,
    COUNT(*) AS num_rows
FROM with_windows_promotions
GROUP BY CUBE (CASE WHEN cumulative_discount_pct_times_prev_discount_pct_bucket >= 90 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cumulative_discount_pct_share_percent_rank >= 0.82 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY num_rows DESC
