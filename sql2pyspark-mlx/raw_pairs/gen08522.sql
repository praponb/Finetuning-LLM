SELECT *, RANK() OVER (PARTITION BY event_type, page ORDER BY duration_sec DESC) AS rank_in_group FROM web_events QUALIFY rank_in_group = 1;
