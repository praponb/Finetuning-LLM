SELECT DISTINCT browser, country
FROM sessions
WHERE HOUR(started_at) BETWEEN 7 AND 13
ORDER BY country DESC, browser
LIMIT 5
