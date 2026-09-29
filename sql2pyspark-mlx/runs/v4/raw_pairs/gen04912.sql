WITH top_web_events AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rnk FROM web_events) SELECT * FROM top_web_events WHERE rnk <= 3;
