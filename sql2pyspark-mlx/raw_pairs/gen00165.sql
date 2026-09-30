SELECT country
FROM sessions
UNION
SELECT status
FROM subscriptions
ORDER BY country;
