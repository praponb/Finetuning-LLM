SELECT s.browser, ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec, MAX(we.event_time) AS latest_event_time
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY s.browser
