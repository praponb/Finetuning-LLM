WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(event_time, 'MMM yyyy') AS event_time_label,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.browser,
        p.device
    FROM base a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.page,
        s.duration_sec,
        s.event_time_hour,
        s.page_clean,
        s.browser,
        p.device AS session_device
    FROM with_dims s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.started_at < '2025-05-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(event_time_hour) OVER (PARTITION BY session_device ORDER BY event_time_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_time_hour,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time_hour, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY session_device, browser ORDER BY duration_sec, id) AS seq_no
    FROM with_dims_web_events
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.browser,
    s.session_device,
    s.running_event_time_hour,
    s.duration_sec_avg_5_prior,
    s.seq_no,
    CASE WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'GROUP_B' ELSE 'CORE' END AS browser_zone,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM with_windows s
ORDER BY page DESC NULLS LAST
