SELECT id, event_time, CASE WHEN duration_sec >= 371 THEN 'High' WHEN duration_sec >= 93 THEN 'Medium' ELSE 'Low' END AS duration_sec_band
FROM web_events
ORDER BY duration_sec_band;
