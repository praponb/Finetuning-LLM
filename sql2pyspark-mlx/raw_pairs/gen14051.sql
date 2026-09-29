SELECT country, browser
FROM sessions
WHERE browser IN ('Firefox', 'Chrome', 'Safari') AND HOUR(started_at) BETWEEN 10 AND 18
ORDER BY browser ASC;
