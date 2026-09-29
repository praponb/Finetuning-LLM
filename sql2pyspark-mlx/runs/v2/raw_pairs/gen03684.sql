SELECT page, MIN(event_time) min_event_time, ROUND(AVG(duration_sec), 2) avg_duration_sec FROM web_events GROUP BY page HAVING ROUND(AVG(duration_sec), 2) >= 57
