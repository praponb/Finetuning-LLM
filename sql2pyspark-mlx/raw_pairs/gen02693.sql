SELECT
    id,
    duration_sec,
    event_time,
    CAST(duration_sec AS STRING) AS duration_sec_str,
    ROUND(duration_sec * 1.1, 2) AS duration_sec_adjusted
FROM web_events
WHERE page = '/checkout' AND event_type = 'page_view';
