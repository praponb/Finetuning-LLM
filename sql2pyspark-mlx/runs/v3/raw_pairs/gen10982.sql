SELECT
    id,
    event_type,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) row_num
FROM web_events
WHERE event_type <> 'click';
