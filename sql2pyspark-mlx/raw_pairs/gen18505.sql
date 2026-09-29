WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        DATEDIFF(ends_on, starts_on) / 7 AS weeks_starts_on_to_ends_on,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on >= DATE '2023-09-20'
      AND channel IS DISTINCT FROM 'partner'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        ends_on,
        discount_pct,
        weeks_starts_on_to_ends_on,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY weeks_starts_on_to_ends_on ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_dense_rank,
        COUNT(weeks_starts_on_to_ends_on) OVER (PARTITION BY channel) AS channel_count_weeks_starts_on_to_ends_on
    FROM cleaned
    WHERE ends_on > '2025-09-12'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        ends_on,
        weeks_starts_on_to_ends_on,
        discount_pct_avg_6_prior,
        channel_count_weeks_starts_on_to_ends_on,
        LEAST(weeks_starts_on_to_ends_on, discount_pct_dense_rank) AS least_weeks_starts_on_to_ends_on_discount_pct_dense_rank,
        LEAST(discount_pct_avg_6_prior, discount_pct) AS least_discount_pct_avg_6_prior_discount_pct
    FROM scored
    WHERE ends_on >= DATE '2023-10-22'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY channel_count_weeks_starts_on_to_ends_on ASC, id ASC) AS channel_count_weeks_starts_on_to_ends_on_bucket,
        CUME_DIST() OVER (ORDER BY channel_count_weeks_starts_on_to_ends_on ASC NULLS LAST) AS channel_count_weeks_starts_on_to_ends_on_cume_dist,
        PERCENT_RANK() OVER (ORDER BY weeks_starts_on_to_ends_on ASC) AS weeks_starts_on_to_ends_on_percent_rank,
        SUM(least_discount_pct_avg_6_prior_discount_pct) OVER (ORDER BY weeks_starts_on_to_ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_discount_pct_avg_6_prior_discount_pct
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    CASE WHEN running_least_discount_pct_avg_6_prior_discount_pct >= 343 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_least_discount_pct_avg_6_prior_discount_pct_tier,
    APPROX_PERCENTILE(least_weeks_starts_on_to_ends_on_discount_pct_dense_rank, 0.9) AS least_weeks_starts_on_to_ends_on_discount_pct_dense_rank_p90,
    PERCENTILE_APPROX(least_discount_pct_avg_6_prior_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS least_discount_pct_avg_6_prior_discount_pct_quartiles,
    ROUND(AVG(weeks_starts_on_to_ends_on), 2) AS avg_weeks_starts_on_to_ends_on,
    COUNT(*) AS cnt,
    GROUPING(DATE_TRUNC('MONTH', ends_on)) AS grouping_level
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', ends_on), CASE WHEN running_least_discount_pct_avg_6_prior_discount_pct >= 343 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('MONTH', ends_on)), ())
HAVING least_weeks_starts_on_to_ends_on_discount_pct_dense_rank_p90 > 0;
