SELECT id, event_type, duration_sec
FROM (
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec) AS row_num
    FROM web_events
) AS t
WHERE row_num = 1
ORDER BY duration_sec DESC, event_type;
