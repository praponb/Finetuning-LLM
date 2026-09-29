SELECT *
FROM (
    SELECT page, MIN(event_time) AS first_event_time, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    WHERE duration_sec > 449
    GROUP BY page
) AS t
WHERE duration_sec_count < 5;
