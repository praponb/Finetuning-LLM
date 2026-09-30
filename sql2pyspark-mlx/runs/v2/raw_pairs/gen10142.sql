SELECT * FROM (SELECT QUARTER(event_time) AS event_quarter, page, duration_sec FROM web_events WHERE HOUR(event_time) BETWEEN 10 AND 16)
PIVOT (MAX(duration_sec) FOR page IN ('/search', '/home'))
ORDER BY event_quarter;
