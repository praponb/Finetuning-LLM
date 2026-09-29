SELECT id, page, duration_sec
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
    WHERE event_type = 'click'
) AS ranked
WHERE pos <= 3
ORDER BY page
