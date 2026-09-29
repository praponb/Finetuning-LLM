SELECT id, started_at
FROM sessions
WHERE browser NOT IN ('Safari', 'Edge', 'Chrome')
ORDER BY 2
