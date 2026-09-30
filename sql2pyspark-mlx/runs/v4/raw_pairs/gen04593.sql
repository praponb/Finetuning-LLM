SELECT
    id,
    page,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS rn
FROM web_events;
