WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(duration_sec, 0) duration_sec_filled,
        COALESCE(page, 'DIRECT') page_clean
    FROM web_events
    WHERE duration_sec = 165
      AND page IS DISTINCT FROM '/checkout'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.event_time,
        src.duration_sec_filled,
        src.session_start_flag,
        p.browser,
        p.device
    FROM sessions_web_events src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.device AS session_device,
        p.country,
        p.browser AS session_browser
    FROM joined s
    INNER JOIN sessions p
        ON s.session_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        session_start_flag,
        browser,
        device,
        session_device,
        LEAD(duration_sec_filled, 1) OVER w_grp next_duration_sec_filled,
        LAST_VALUE(session_device) IGNORE NULLS OVER (ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_session_device
    FROM joined_web_events
    WINDOW w_grp AS (PARTITION BY browser ORDER BY session_start_flag ASC, id ASC)
)
SELECT
    id,
    page,
    session_start_flag,
    browser,
    device,
    session_device,
    last_known_session_device,
    CASE WHEN device RLIKE '^(desktop|tablet)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS device_zone
FROM metrics
ORDER BY session_device DESC NULLS LAST, device DESC;
