SELECT event_type, unique_pages, avg_duration_sec
FROM (
    SELECT event_type, COUNT(DISTINCT duration_sec) AS unique_pages, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY event_type
) AS agg
WHERE unique_pages >= 7;
