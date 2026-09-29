SELECT country
FROM sessions
WHERE HOUR(started_at) BETWEEN 6 AND 12
UNION
SELECT country
FROM sessions
WHERE browser NOT IN ('Chrome', 'Safari');
