SELECT DISTINCT country, browser
FROM sessions
ORDER BY browser, country
LIMIT 3
