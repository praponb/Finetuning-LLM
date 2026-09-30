SELECT id, country
FROM sessions
WHERE device IN ('tablet', 'desktop')
ORDER BY 1 DESC
LIMIT 20
