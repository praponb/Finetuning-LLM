SELECT page, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, MIN(duration_sec) AS min_duration_sec FROM web_events WHERE duration_sec = 132 GROUP BY page;
