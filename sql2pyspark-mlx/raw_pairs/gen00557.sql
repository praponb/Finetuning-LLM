SELECT country, device, started_at
FROM sessions
WHERE HOUR(started_at) BETWEEN 6 AND 10
ORDER BY device, started_at DESC;
