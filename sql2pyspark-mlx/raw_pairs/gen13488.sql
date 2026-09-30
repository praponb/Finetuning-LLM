SELECT s.country, COUNT(we.duration_sec) AS duration_sec_count FROM web_events we JOIN sessions s ON s.id = we.session_id WHERE s.country IN ('SG', 'US') GROUP BY s.country;
