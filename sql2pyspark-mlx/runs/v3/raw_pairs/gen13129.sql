WITH session_totals AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
    FROM web_events
    GROUP BY session_id
)
SELECT s.country, s.started_at, st.event_type_values
FROM sessions s
JOIN session_totals st ON s.id = st.session_id;
