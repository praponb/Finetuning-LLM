SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY event_time DESC, id) AS dedup_rank
    FROM web_events
) t
WHERE dedup_rank = 1;
