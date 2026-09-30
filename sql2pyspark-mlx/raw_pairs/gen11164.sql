SELECT * FROM (SELECT id, event_type, event_time, page, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rnk FROM web_events) AS r WHERE rnk <= 2 ORDER BY id DESC;
