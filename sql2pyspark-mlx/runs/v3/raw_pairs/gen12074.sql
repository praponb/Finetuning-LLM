SELECT id, start_date, customer_id
FROM subscriptions
WHERE start_date = (SELECT MIN(start_date) FROM subscriptions)
ORDER BY id, customer_id DESC
