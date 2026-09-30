WITH session_summary AS (
    SELECT session_id, COUNT(duration_sec) AS duration_sec_count, COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.browser,
    ss.duration_sec_count,
    ss.purchase_count
FROM sessions s
LEFT OUTER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY browser;
