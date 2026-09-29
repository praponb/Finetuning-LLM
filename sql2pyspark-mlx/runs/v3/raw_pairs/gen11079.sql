SELECT id, customer_id, COUNT(*) OVER (PARTITION BY customer_id) AS customer_id_count
FROM subscriptions
WHERE customer_id IS NOT NULL;
