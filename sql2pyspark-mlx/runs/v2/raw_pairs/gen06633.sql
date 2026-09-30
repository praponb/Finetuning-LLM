SELECT status, customer_id
FROM subscriptions
WHERE start_date < '2025-06-01'
ORDER BY customer_id DESC
