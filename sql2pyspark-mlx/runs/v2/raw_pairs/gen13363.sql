WITH WebEventsBySession AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    W.event_type_values,
    W.min_duration_sec
FROM sessions s
JOIN WebEventsBySession W ON s.id = W.session_id
WHERE W.min_duration_sec > 350
