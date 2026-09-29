SELECT id, session_id, event_type FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time ASC NULLS FIRST, id ASC) AS row_num FROM web_events) x WHERE row_num = 1
