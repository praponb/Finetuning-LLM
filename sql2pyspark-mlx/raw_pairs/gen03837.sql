SELECT event_type, COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count, COUNT(duration_sec) AS duration_sec_count FROM web_events GROUP BY event_type;
