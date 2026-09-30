WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CONCAT_WS('/', device, browser) AS device_browser_key,
        UPPER(browser) AS browser_upper
    FROM sessions
    WHERE started_at >= DATE '2023-05-11'
      AND country IS NOT DISTINCT FROM 'JP'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        browser_upper
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC NULLS FIRST, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (PARTITION BY device, browser ORDER BY started_at, id) AS event_seq,
        FIRST_VALUE(browser, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
    FROM latest
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.started_at,
        s.browser_upper,
        s.last_known_browser,
        top1.web_event_duration_sec
    FROM scored s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Window metrics over several different partitions
SELECT
    id,
    device,
    browser_upper,
    last_known_browser,
    AVG(web_event_duration_sec) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_web_event_duration_sec,
    MIN(web_event_duration_sec) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_web_event_duration_sec,
    COUNT(web_event_duration_sec) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_web_event_duration_sec,
    SUM(web_event_duration_sec) OVER (PARTITION BY device ORDER BY web_event_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_web_event_duration_sec
FROM with_last
WHERE customer_id IN (3, 22, 30);
