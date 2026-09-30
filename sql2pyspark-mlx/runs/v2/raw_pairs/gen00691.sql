SELECT
    event_type,
    QUARTER(event_time) AS event_quarter,
    session_id,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE event_type = 'click' AND (session_id = 4 OR duration_sec != 389)
GROUP BY event_type, QUARTER(event_time), session_id
