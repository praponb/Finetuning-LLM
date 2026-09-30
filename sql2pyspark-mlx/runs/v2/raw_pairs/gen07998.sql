SELECT YEAR(event_time) AS event_year, SUM(duration_sec) AS total_duration_sec, SUM(SUM(duration_sec)) OVER (ORDER BY YEAR(event_time)) AS cumulative_duration_sec
FROM web_events
WHERE event_type = 'purchase'
GROUP BY YEAR(event_time)
ORDER BY cumulative_duration_sec DESC
