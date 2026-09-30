WITH web_events_by_session AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.country,
    webs.max_duration_sec,
    webs.avg_duration_sec
FROM sessions s
LEFT JOIN web_events_by_session webs ON s.id = webs.session_id
ORDER BY webs.max_duration_sec
