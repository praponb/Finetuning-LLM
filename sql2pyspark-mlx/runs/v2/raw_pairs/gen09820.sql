SELECT browser
FROM sessions
WHERE started_at < '2025-07-27'
UNION ALL
SELECT browser
FROM sessions
WHERE device = 'tablet'
ORDER BY browser
