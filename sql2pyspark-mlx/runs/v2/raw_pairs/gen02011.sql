WITH session_summary AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    ss.max_duration_sec,
    ss.event_type_values
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.max_duration_sec >= 190;
