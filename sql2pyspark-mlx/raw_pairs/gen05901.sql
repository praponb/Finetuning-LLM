SELECT * FROM (SELECT event_type, MIN(duration_sec) AS min_duration_sec FROM web_events GROUP BY event_type) AS x WHERE min_duration_sec < 260 ORDER BY event_type;
