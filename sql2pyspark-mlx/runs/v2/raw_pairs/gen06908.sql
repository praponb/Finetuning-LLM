SELECT TO_DATE(event_time) event_day, QUARTER(event_time) event_quarter, MIN(duration_sec) min_duration_sec
FROM web_events
WHERE event_type = 'add_to_cart' AND page IS NOT NULL
GROUP BY TO_DATE(event_time), QUARTER(event_time);
