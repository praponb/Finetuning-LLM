SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events) src PIVOT (AVG(duration_sec) AS amount, COUNT(1) AS n FOR page IN ('/cart' AS cart, '/search' AS search));
