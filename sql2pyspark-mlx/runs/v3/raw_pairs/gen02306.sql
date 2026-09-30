SELECT s.customer_id, MIN(we.event_time) AS first_event_time, ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
GROUP BY s.customer_id
ORDER BY stddev_duration_sec DESC, first_event_time
