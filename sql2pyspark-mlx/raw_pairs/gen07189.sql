SELECT status, COUNT(*) AS cnt
FROM subscriptions
GROUP BY status
HAVING cnt > 1
ORDER BY cnt DESC
