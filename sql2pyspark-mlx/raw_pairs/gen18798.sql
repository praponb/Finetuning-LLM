SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC NULLS FIRST, id) AS rank_in_group
FROM web_events
QUALIFY rank_in_group = 1;
