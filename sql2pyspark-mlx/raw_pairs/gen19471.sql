WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        CASE WHEN device IN ('mobile', 'desktop') THEN 'Core' ELSE 'Other' END AS device_group,
        CASE WHEN browser IN ('Chrome', 'Edge') THEN 'Tier 1' ELSE 'Other' END AS browser_group,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM sessions
    WHERE started_at >= TO_DATE('2023-06-11')
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_event_type
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        started_at,
        browser_group,
        resolved_browser,
        web_event_event_type,
        FIRST_VALUE(browser, TRUE) OVER w_running AS first_known_browser
    FROM with_last
    WHERE browser IN ('Chrome', 'Firefox', 'Edge')
    WINDOW w_running AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.segment,
        p.email,
        p.name
    FROM metrics e
    JOIN customers p
        ON e.customer_id = p.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_duration_sec
    FROM islands s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    browser_group,
    web_event_event_type,
    first_known_browser,
    name,
    session_start_flag,
    CASE WHEN name RLIKE '^(Noah Park|Mia Rossi)$' THEN 'GROUP_B' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS name_zone
FROM not_flagged
ORDER BY name_zone DESC NULLS FIRST
