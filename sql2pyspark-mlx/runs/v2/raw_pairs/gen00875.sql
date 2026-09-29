SELECT s.device, ROUND(AVG(we.duration_sec), 2) AS avg_duration_sec, MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON c.id = s.customer_id
WHERE s.browser NOT IN ('Chrome', 'Safari', 'Edge')
GROUP BY s.device
