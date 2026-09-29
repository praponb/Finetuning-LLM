SELECT id, started_at
FROM sessions
WHERE browser = 'Safari' AND device NOT IN ('desktop', 'mobile', 'tablet') AND HOUR(started_at) BETWEEN 12 AND 17
ORDER BY started_at, id
LIMIT 5
