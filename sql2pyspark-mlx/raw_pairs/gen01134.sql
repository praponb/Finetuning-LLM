WITH base AS (
    SELECT
        *,
        CASE WHEN channel = 'web' THEN 1 ELSE 0 END AS is_web,
        LENGTH(channel) AS channel_length
    FROM promotions
    WHERE YEAR(starts_on) = 2025
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        channel_length,
        latest_rank,
        NTILE(3) OVER (PARTITION BY MONTH(ends_on) ORDER BY channel_length, id) AS channel_length_bucket,
        LAST_VALUE(is_web, TRUE) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_web,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct
    FROM deduped
),
enriched AS (
    SELECT
        id,
        ends_on,
        latest_rank,
        running_discount_pct,
        LEAD(running_discount_pct, 1) OVER (ORDER BY ends_on, id) AS next_running_discount_pct,
        SUM(channel_length_bucket) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_channel_length_bucket
    FROM metrics
)
SELECT
    CASE WHEN cumulative_channel_length_bucket >= 42 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_channel_length_bucket_tier,
    CASE WHEN next_running_discount_pct >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_running_discount_pct_tier,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(running_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS running_discount_pct_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY CASE WHEN cumulative_channel_length_bucket >= 42 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_running_discount_pct >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY next_running_discount_pct_tier DESC NULLS LAST;
