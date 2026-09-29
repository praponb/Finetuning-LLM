SELECT YEAR(event_time) AS event_year, AVG(duration_sec) AS avg_duration_sec FROM web_events GROUP BY YEAR(event_time) HAVING avg_duration_sec < 320;
