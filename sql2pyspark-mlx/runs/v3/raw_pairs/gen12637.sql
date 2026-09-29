SELECT
    id,
    event_type,
    duration_sec,
    DENSE_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS dense_rnk
FROM web_events
ORDER BY id DESC;
