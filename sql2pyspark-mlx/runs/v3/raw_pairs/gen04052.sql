SELECT id, session_id, event_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS rnk FROM web_events) AS ranked WHERE rnk = 1;
