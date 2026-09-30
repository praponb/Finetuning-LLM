SELECT page, SUM(duration_sec) AS total_duration_sec, MAX(duration_sec) AS max_duration_sec FROM web_events GROUP BY page
