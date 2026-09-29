SELECT id, customer_id
FROM subscriptions
WHERE end_date > '2023-12-27' OR plan = 'premium'
