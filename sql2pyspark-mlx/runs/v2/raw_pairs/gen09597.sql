WITH web_events_by_session AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.device, webs.stddev_duration_sec
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
ORDER BY device
