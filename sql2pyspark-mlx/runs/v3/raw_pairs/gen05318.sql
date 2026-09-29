WITH SessionSummary AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.country,
    s.browser,
    S.event_type_values,
    S.max_duration_sec
FROM sessions s
JOIN SessionSummary S ON s.id = S.session_id
ORDER BY max_duration_sec
