SELECT c.segment, MAX(s.started_at) AS latest_started_at, COUNT(DISTINCT s.device) AS unique_devices
FROM sessions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
GROUP BY c.segment
ORDER BY latest_started_at
