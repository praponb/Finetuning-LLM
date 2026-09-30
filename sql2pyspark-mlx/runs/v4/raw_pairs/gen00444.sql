WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUBSTRING(page, 1, 4) AS page_prefix,
        COALESCE(page, 'NONE') AS resolved_page
    FROM web_events
    WHERE event_time > DATE '2024-04-02'
),
with_dims AS (
    SELECT
        e.*,
        b.browser,
        b.country,
        b.device
    FROM base e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
base_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_prefix,
        resolved_page,
        browser,
        device,
        CASE WHEN event_type IN ('purchase', 'page_view') THEN 'Priority' ELSE 'Other' END AS event_type_group
    FROM with_dims
    WHERE country IN ('US', 'TH', 'JP')
)
SELECT
    *,
    LAG(duration_sec, 1) OVER (PARTITION BY browser ORDER BY duration_sec, id) AS prev_duration_sec,
    NTILE(5) OVER (PARTITION BY device ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
    NTILE(5) OVER (PARTITION BY browser ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2
FROM base_web_events
