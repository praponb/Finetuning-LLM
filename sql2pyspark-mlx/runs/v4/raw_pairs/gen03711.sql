WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        HOUR(started_at) AS started_at_hour,
        COALESCE(browser, 'OTHER') AS browser_clean
    FROM sessions
    WHERE started_at >= DATE '2023-07-26'
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, c.city
    FROM base src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
enriched_sessions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        started_at_hour,
        browser_clean,
        city,
        LAST_VALUE(started_at_hour) IGNORE NULLS OVER (PARTITION BY city, device ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_started_at_hour,
        NTILE(10) OVER (ORDER BY started_at_hour DESC, id) AS started_at_hour_bucket
    FROM enriched
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.segment,
        b.country,
        b.email
    FROM enriched_sessions a
    INNER JOIN customers b
        ON a.customer_id = b.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.page
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    browser,
    CASE WHEN last_known_started_at_hour >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_started_at_hour_tier,
    MAX(started_at_hour_bucket) FILTER (WHERE browser = 'Chrome') AS chrome_max_started_at_hour_bucket,
    ROUND(AVG(last_known_started_at_hour), 2) AS avg_last_known_started_at_hour,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    APPROX_PERCENTILE(started_at_hour_bucket, 0.5) AS started_at_hour_bucket_p50,
    MIN(started_at) AS started_at
FROM latest_match
GROUP BY 1, 2
HAVING avg_last_known_started_at_hour < 0
ORDER BY consumer_count NULLS LAST
