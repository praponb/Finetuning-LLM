WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        LENGTH(promo_code) AS promo_code_length,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE ends_on > DATE '2023-08-02'
      AND channel IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY ends_on) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        channel_filled,
        latest_rank,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY promo_code_length DESC NULLS FIRST) AS promo_code_length_cume_dist,
        SUM(promo_code_length) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promo_code_length,
        SUM(promo_code_length) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_promo_code_length,
        DENSE_RANK() OVER (PARTITION BY YEAR(starts_on), promo_code ORDER BY promo_code_length NULLS FIRST) AS promo_code_length_dense_rank,
        LAG(promo_code_length, 1) OVER (PARTITION BY DATE_TRUNC('month', starts_on), channel ORDER BY promo_code_length, id) AS prev_promo_code_length
    FROM ranked
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        channel_filled,
        promo_code_length_cume_dist,
        cumulative_promo_code_length,
        prev_promo_code_length,
        PERCENT_RANK() OVER (ORDER BY promo_code_length_cume_dist DESC NULLS FIRST) AS promo_code_length_cume_dist_percent_rank,
        AVG(promo_code_length_cume_dist) OVER (PARTITION BY MONTH(starts_on), channel ORDER BY running_promo_code_length, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS promo_code_length_cume_dist_avg_2_prior
    FROM with_windows
    WHERE cumulative_promo_code_length = 351
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel_filled,
        promo_code_length_cume_dist,
        cumulative_promo_code_length,
        promo_code_length_cume_dist_avg_2_prior,
        LEAD(promo_code_length_cume_dist_percent_rank) OVER (PARTITION BY channel ORDER BY promo_code_length_cume_dist_avg_2_prior, id) AS next_promo_code_length_cume_dist_percent_rank,
        LEAD(promo_code_length_cume_dist_avg_2_prior) OVER (PARTITION BY channel ORDER BY promo_code_length_cume_dist_avg_2_prior, id) AS next_promo_code_length_cume_dist_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promo_code_length_cume_dist_avg_2_prior, id) AS event_seq
    FROM with_windows_promotions
)
-- Grouped metrics
SELECT
    CASE WHEN promo_code_length_cume_dist >= 0.81 THEN 'LARGE' ELSE 'SMALL' END AS promo_code_length_cume_dist_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(next_promo_code_length_cume_dist_avg_2_prior), 2) AS avg_next_promo_code_length_cume_dist_avg_2_prior,
    APPROX_PERCENTILE(next_promo_code_length_cume_dist_avg_2_prior, ARRAY(0.25, 0.5, 0.75)) AS next_promo_code_length_cume_dist_avg_2_prior_quartiles,
    ROUND(AVG(promo_code_length_cume_dist), 2) AS avg_promo_code_length_cume_dist,
    MAX(next_promo_code_length_cume_dist_percent_rank) AS max_next_promo_code_length_cume_dist_percent_rank
FROM enriched
GROUP BY 1
ORDER BY distinct_ids DESC;
