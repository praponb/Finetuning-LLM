WITH web_events_by_session AS (
    SELECT session_id, COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    WHERE duration_sec BETWEEN 160 AND 266
    GROUP BY session_id
)
SELECT
    s.browser,
    s.started_at,
    webs.checkout_count,
    webs.max_duration_sec
FROM sessions s
LEFT JOIN web_events_by_session webs ON s.id = webs.session_id
