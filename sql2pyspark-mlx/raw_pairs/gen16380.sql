SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS dedup_rank
FROM web_events
QUALIFY dedup_rank = 1
