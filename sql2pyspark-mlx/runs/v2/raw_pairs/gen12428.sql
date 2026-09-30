SELECT
    id,
    duration_sec,
    event_time,
    MONTH(event_time) AS event_time_month,
    COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
WHERE duration_sec BETWEEN 178 AND 292 AND duration_sec BETWEEN 276 AND 347
