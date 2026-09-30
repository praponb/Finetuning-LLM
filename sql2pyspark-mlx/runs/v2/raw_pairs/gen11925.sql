SELECT id, end_date, plan
FROM subscriptions
WHERE end_date = (SELECT MAX(end_date) FROM subscriptions)
ORDER BY end_date
