SELECT event_year, max_duration_sec, first_event_time
FROM (
    SELECT YEAR(event_time) AS event_year, MAX(duration_sec) AS max_duration_sec, MIN(event_time) AS first_event_time
    FROM web_events
    WHERE duration_sec BETWEEN 276 AND 431
    GROUP BY YEAR(event_time)
) AS s
WHERE max_duration_sec < 220;
