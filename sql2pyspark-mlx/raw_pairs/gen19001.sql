SELECT page, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE NOT (event_type NOT IN ('add_to_cart', 'purchase', 'click') OR event_type = 'click')
GROUP BY page
HAVING avg_duration_sec > 21
ORDER BY avg_duration_sec;
