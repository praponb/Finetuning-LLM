WITH ranked_web_events AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS pos FROM web_events) SELECT * FROM ranked_web_events WHERE pos <= 3
