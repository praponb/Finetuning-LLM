SELECT sessions.country, COUNT(web_events.id) web_event_count, SUM(web_events.duration_sec) total_duration_sec
FROM sessions
LEFT JOIN web_events ON web_events.session_id = sessions.id
WHERE TO_DATE(web_events.event_time) >= '2024-11-27'
GROUP BY sessions.country;
