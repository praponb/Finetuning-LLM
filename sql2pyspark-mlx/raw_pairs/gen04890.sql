SELECT id, end_date, status
FROM subscriptions
WHERE end_date = (SELECT MIN(end_date) FROM subscriptions)
