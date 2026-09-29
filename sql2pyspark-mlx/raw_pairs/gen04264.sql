WITH web_events_by_session AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) AS click_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.device, webs.click_duration_sec
FROM sessions s
LEFT JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.click_duration_sec > 52;
