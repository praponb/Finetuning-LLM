WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('month', ends_on) AS ends_on_period
    FROM promotions
    WHERE starts_on > TO_DATE('2023-05-26')
      AND discount_pct >= 0.29
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on_period,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY MONTH(ends_on_period), promo_code), 4) AS discount_pct_share,
        MAX(discount_pct) OVER (PARTITION BY channel ORDER BY CAST(ends_on_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS discount_pct_max_7d,
        NTILE(4) OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        LAG(discount_pct, 1) OVER (PARTITION BY channel ORDER BY ends_on, id) AS prev_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior
    FROM cleaned
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY prev_discount_pct, id) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        starts_on,
        discount_pct_share,
        discount_pct_max_7d,
        discount_pct_bucket,
        discount_pct_avg_2_prior,
        LAST_VALUE(discount_pct_avg_2_prior) IGNORE NULLS OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct_share DESC NULLS LAST) AS discount_pct_share_dense_rank,
        AVG(discount_pct_avg_2_prior) OVER (PARTITION BY channel ORDER BY discount_pct_avg_2_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_pct_avg_2_prior_avg_3,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY prev_discount_pct DESC) AS prev_discount_pct_percent_rank,
        LEAD(discount_pct_max_7d, 1) OVER (PARTITION BY channel ORDER BY discount_pct_max_7d, id) AS next_discount_pct_max_7d
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', starts_on) AS activity_month,
    CASE WHEN prev_discount_pct_percent_rank >= 0.84 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_discount_pct_percent_rank_tier,
    SUM(discount_pct_max_7d) AS total_discount_pct_max_7d,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(prev_discount_pct_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS prev_discount_pct_percent_rank_quartiles,
    GROUPING(DATE_TRUNC('MONTH', starts_on)) AS grouping_id
FROM scored
GROUP BY ROLLUP (DATE_TRUNC('MONTH', starts_on), CASE WHEN prev_discount_pct_percent_rank >= 0.84 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY activity_month DESC
