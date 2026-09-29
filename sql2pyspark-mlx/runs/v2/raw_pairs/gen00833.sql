SELECT event_type, COUNT(*) AS cnt, COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count
FROM web_events
GROUP BY event_type;
