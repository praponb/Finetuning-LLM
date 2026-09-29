SELECT start_date, customer_id
FROM subscriptions
WHERE customer_id IN (10, 19, 33, 39) AND plan = 'premium';
