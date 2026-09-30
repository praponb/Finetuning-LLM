SELECT
    page,
    event_type,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count
FROM web_events
WHERE session_id = 12
GROUP BY CUBE(page, event_type);
