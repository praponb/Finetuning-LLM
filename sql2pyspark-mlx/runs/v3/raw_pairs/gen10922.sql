SELECT s.device, ROUND((100 * SUM(CASE WHEN we.event_type = 'purchase' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_purchase
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.duration_sec BETWEEN 285 AND 334
GROUP BY s.device
HAVING ROUND((100 * SUM(CASE WHEN we.event_type = 'purchase' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 79
