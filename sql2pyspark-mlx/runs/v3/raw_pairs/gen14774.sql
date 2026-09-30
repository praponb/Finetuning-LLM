WITH web_events_by_session AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.started_at,
    webs.duration_sec_range
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.duration_sec_range > 70
