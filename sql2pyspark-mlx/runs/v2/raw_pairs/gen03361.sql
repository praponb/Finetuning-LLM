SELECT id, end_date, start_date
FROM subscriptions
WHERE end_date = (SELECT MAX(end_date) FROM subscriptions);
