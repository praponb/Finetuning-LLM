WITH with_latest AS (
    SELECT
        s.*,
        x.web_event_event_time,
        x.duration_sec
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_latest
),
sessions_sessions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_latest_sessions AS (
    SELECT
        s.id,
        s.browser,
        x.web_event_duration_sec,
        x.web_event_page
    FROM sessions_sessions s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.web_event_event_time
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    SELECT id, browser
    FROM with_latest_sessions
    EXCEPT DISTINCT
    SELECT id, browser
    FROM sessions
    WHERE customer_id = 23
),
verified AS (
    SELECT s.*
    FROM with_latest_sessions s
    INNER JOIN kept_keys k ON s.id = k.id AND s.browser = k.browser
)
SELECT
    CASE WHEN web_event_duration_sec >= 393 THEN 'LARGE' ELSE 'SMALL' END AS web_event_duration_sec_tier,
    ROUND(SUM(web_event_duration_sec) / NULLIF(COUNT(CASE WHEN browser = 'Edge' THEN 1 END), 0), 2) AS web_event_duration_sec_per_edge,
    ROUND(AVG(web_event_duration_sec), 2) AS avg_web_event_duration_sec
FROM verified
GROUP BY web_event_duration_sec_tier
HAVING ROUND(AVG(web_event_duration_sec), 2) > 0;
