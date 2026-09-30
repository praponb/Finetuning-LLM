WITH with_latest AS (
    SELECT
        s.id,
        s.device,
        s.browser,
        s.started_at,
        top1.web_event_page
    FROM sessions s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    SELECT
        id,
        device,
        browser,
        started_at,
        web_event_page,
        DATEDIFF(CURRENT_DATE(), started_at) / 30 AS months_since_started_at
    FROM with_latest
    WHERE browser IS NOT NULL
      AND web_event_page IS DISTINCT FROM '/cart'
),
latest_match AS (
    SELECT
        s.*,
        latest.event_time,
        latest.page
    FROM filtered s
    JOIN LATERAL (
            SELECT c.event_time, c.page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY web_event_page ORDER BY months_since_started_at DESC, id) = 1
),
cleaned AS (
    SELECT
        id,
        device,
        started_at,
        web_event_page,
        event_time,
        COALESCE(web_event_page, 'Unknown') AS web_event_page_filled
    FROM deduped
    WHERE device IN ('mobile', 'desktop')
),
summary AS (
    SELECT
        DATE_TRUNC('WEEK', event_time) AS week_start,
        MAX(event_time) AS last_event_time,
        MIN(started_at) AS started_at,
        MAX(event_time) AS last_seen,
        COUNT_IF(device = 'mobile') AS mobile_events
    FROM cleaned
    GROUP BY DATE_TRUNC('WEEK', event_time)
    HAVING COUNT_IF(device = 'mobile') < 1000
)
SELECT
    week_start,
    last_event_time,
    started_at,
    last_seen
FROM summary
ORDER BY last_seen DESC NULLS FIRST
