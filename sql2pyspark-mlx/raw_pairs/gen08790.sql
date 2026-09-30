WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        SUBSTRING(page, 1, 3) AS page_prefix,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
      AND event_time >= TIMESTAMP '2023-09-29 10:00:00'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        page,
        page_prefix,
        LAG(duration_sec, 2) OVER w_part AS prev_duration_sec,
        NTILE(10) OVER w_part AS duration_sec_bucket
    FROM cleaned
    WINDOW w_part AS (PARTITION BY page ORDER BY duration_sec, id)
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT e.*, s.device
    FROM enriched e
    INNER JOIN sessions s
        ON e.session_id = s.id
    WHERE s.device IN ('desktop', 'tablet', 'mobile')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        duration_sec_bucket,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_bucket, id) AS row_seq,
        AVG(duration_sec_bucket) OVER w_running AS duration_sec_bucket_avg_2_prior,
        NTILE(3) OVER (PARTITION BY session_id ORDER BY duration_sec_bucket DESC, id) AS duration_sec_bucket_bucket,
        NTILE(4) OVER (PARTITION BY page, session_id ORDER BY duration_sec_bucket, id) AS duration_sec_bucket_bucket_2,
        ROUND(duration_sec_bucket / SUM(duration_sec_bucket) OVER (PARTITION BY device, session_id), 4) AS duration_sec_bucket_share
    FROM enriched_web_events
    WINDOW w_running AS (PARTITION BY page ORDER BY prev_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        page,
        duration_sec_bucket,
        duration_sec_bucket_avg_2_prior,
        duration_sec_bucket_bucket,
        duration_sec_bucket_bucket_2,
        duration_sec_bucket_share,
        ROUND(duration_sec_bucket_share, 0) AS duration_sec_bucket_share_rounded,
        LEAST(duration_sec_bucket_bucket_2, duration_sec_bucket_avg_2_prior) AS least_duration_sec_bucket_bucket_2_duration_sec_bucket_avg_2_prior,
        COALESCE(page, 'N/A') AS page_filled
    FROM with_windows
    WHERE page IS NOT NULL
      AND duration_sec_bucket_bucket_2 > 6
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec_bucket_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_bucket_bucket_tier,
    PERCENTILE_APPROX(duration_sec_bucket, 0.95) AS duration_sec_bucket_p95,
    COUNT_IF(page = '/checkout') AS checkout_events,
    ROUND(AVG(duration_sec_bucket_bucket_2), 2) AS avg_duration_sec_bucket_bucket_2
FROM base
GROUP BY 1
HAVING avg_duration_sec_bucket_bucket_2 >= 0
