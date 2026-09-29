SELECT * FROM web_events QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) = 1
