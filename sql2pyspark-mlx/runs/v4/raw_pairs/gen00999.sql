WITH web_events_by_session AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec
    FROM web_events
    WHERE session_id IN (1, 3, 5, 20)
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.started_at,
    webs.stddev_duration_sec
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.stddev_duration_sec > 72
ORDER BY started_at DESC, id
LIMIT 50;
