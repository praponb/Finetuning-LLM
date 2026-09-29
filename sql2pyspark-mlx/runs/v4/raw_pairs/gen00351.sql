WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(country, 'unassigned') AS country_filled,
        COALESCE(country, 'DIRECT') AS country_filled_2
    FROM sessions
    WHERE country NOT IN ('JP', 'SG')
),
base_sessions AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(browser)) AS browser_title
    FROM base
    WHERE browser IS NOT NULL
      AND device IN ('desktop', 'tablet', 'mobile')
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.duration_sec,
        x.web_event_page
    FROM base_sessions s
    JOIN LATERAL (
            SELECT c.duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.page,
        x.event_time
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    device,
    started_at,
    country_filled,
    country_filled_2,
    web_event_page,
    event_time,
    CASE WHEN device RLIKE '^(desktop|mobile)$' THEN 'GROUP_A' WHEN device RLIKE '^(tablet)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS device_zone
FROM with_latest
ORDER BY device_zone DESC;
