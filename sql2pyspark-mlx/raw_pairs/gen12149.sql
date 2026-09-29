SELECT id, event_time, ROW_NUMBER() OVER (ORDER BY event_time, id) AS row_num FROM web_events
