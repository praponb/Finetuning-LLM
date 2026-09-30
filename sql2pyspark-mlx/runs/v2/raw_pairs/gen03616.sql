SELECT
    MONTH(event_time) AS event_month,
    page,
    MAX(event_time) AS latest_event_time,
    AVG(duration_sec) AS avg_duration_sec,
    MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE event_time < '2023-05-01' AND page IN ('/checkout', '/home', '/search')
GROUP BY CUBE(MONTH(event_time), page)
ORDER BY min_duration_sec, avg_duration_sec;
