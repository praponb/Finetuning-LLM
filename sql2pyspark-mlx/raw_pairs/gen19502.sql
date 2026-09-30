WITH with_windows AS (
    SELECT
        *,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_duration_sec,
        NTILE(3) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS duration_sec_bucket
    FROM web_events
),
enriched AS (
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.duration_sec,
        s.last4_count_duration_sec,
        s.duration_sec_bucket,
        p.browser,
        p.country,
        p.device
    FROM with_windows s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
metrics AS (
    SELECT
        id,
        event_type,
        page,
        browser,
        country,
        device,
        NTILE(3) OVER (PARTITION BY country, device ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2,
        NTILE(10) OVER (ORDER BY duration_sec_bucket DESC, id) AS duration_sec_bucket_bucket,
        AVG(last4_count_duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last4_count_duration_sec_avg_2_prior
    FROM enriched
),
with_windows_web_events AS (
    SELECT
        id,
        page,
        browser,
        country,
        duration_sec_bucket_bucket,
        SUM(last4_count_duration_sec_avg_2_prior) OVER (ORDER BY duration_sec_bucket_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last4_count_duration_sec_avg_2_prior,
        LAST_VALUE(device) IGNORE NULLS OVER (PARTITION BY browser ORDER BY last4_count_duration_sec_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device,
        RANK() OVER (PARTITION BY device ORDER BY duration_sec_bucket_bucket) AS duration_sec_bucket_bucket_rank
    FROM metrics
),
valid AS (
    SELECT
        id,
        page,
        browser,
        duration_sec_bucket_bucket,
        last_known_device,
        duration_sec_bucket_bucket_rank,
        LOWER(country) AS country_lower
    FROM with_windows_web_events
    WHERE country IS NOT NULL
      AND page = '/product'
)
SELECT
    id,
    page,
    duration_sec_bucket_bucket,
    last_known_device,
    duration_sec_bucket_bucket_rank
FROM valid;
