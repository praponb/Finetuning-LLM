WITH SessionStats AS (
    SELECT session_id, COUNT(*) AS total_count, MAX(event_time) AS latest_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.country,
    s.device,
    S.total_count,
    S.latest_event_time
FROM sessions s
INNER JOIN SessionStats S ON s.id = S.session_id
WHERE S.total_count > 8;
