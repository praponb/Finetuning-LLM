SELECT id, session_id, event_type FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rank_in_group FROM web_events) t WHERE rank_in_group = 1
