WITH web_events_by_session AS (
    SELECT session_id, COUNT(duration_sec) duration_sec_count
    FROM web_events
    WHERE session_id = 13
    GROUP BY session_id
)
SELECT s.browser, webs.duration_sec_count
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.duration_sec_count >= 6
