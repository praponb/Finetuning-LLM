WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        country,
        INITCAP(TRIM(browser)) AS browser_title
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-04-04 00:00:00')
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id = 27
        )
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.browser,
        x.page
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.page,
        top1.web_event_event_time
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (5, 39, 40)
        )
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN web_event_event_time - LAG(web_event_event_time) OVER (PARTITION BY page ORDER BY web_event_event_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM not_flagged_sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY web_event_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
    session_no,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    COUNT(DISTINCT device) AS distinct_devices,
    SIZE(COLLECT_SET(page)) AS num_pages,
    COUNT_IF(device = 'mobile') AS mobile_events,
    ROUND(AVG(new_group_flag), 2) AS avg_new_group_flag,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN page = '/product' THEN 1 END), 0), 2) AS session_no_per_product
FROM sessionized
GROUP BY 1, 2;
