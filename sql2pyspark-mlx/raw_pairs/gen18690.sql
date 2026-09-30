WITH web_events_by_session AS (
    SELECT session_id, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.browser,
    webs.min_duration_sec
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.min_duration_sec > 260
