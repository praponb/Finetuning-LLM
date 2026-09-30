SELECT DISTINCT plan, status
FROM subscriptions
WHERE MONTH(end_date) IN (1, 2, 8)
ORDER BY plan
