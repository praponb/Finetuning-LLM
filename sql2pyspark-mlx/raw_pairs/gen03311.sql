SELECT
    page,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values,
    SUM(duration_sec) AS total_duration_sec,
    MAX(duration_sec) AS max_duration_sec
FROM web_events
GROUP BY page;
