SELECT page, MIN(event_time) first_event_time, SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) click_duration_sec
FROM web_events
GROUP BY 1
HAVING SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) < 34
ORDER BY page;
