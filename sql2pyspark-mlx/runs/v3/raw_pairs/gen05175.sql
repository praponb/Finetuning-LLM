WITH top_web_events AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS row_num FROM web_events) SELECT * FROM top_web_events WHERE row_num <= 3;
