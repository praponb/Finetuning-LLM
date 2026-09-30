SELECT event_type, ROUND(AVG(duration_sec), 2) avg_duration_sec, COUNT(*) row_count
FROM web_events
GROUP BY 1
ORDER BY row_count DESC, avg_duration_sec DESC, event_type ASC
LIMIT 5
