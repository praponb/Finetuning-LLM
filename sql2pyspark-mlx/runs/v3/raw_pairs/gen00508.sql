SELECT s.browser, MAX(we.duration_sec) AS max_duration_sec FROM web_events we LEFT JOIN sessions s ON s.id = we.session_id LEFT JOIN customers c ON c.id = s.customer_id GROUP BY s.browser;
