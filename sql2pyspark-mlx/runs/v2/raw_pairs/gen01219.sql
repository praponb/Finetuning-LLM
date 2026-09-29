SELECT id, start_date, monthly_fee
FROM subscriptions
WHERE customer_id IS NOT NULL AND customer_id IN (23, 38) AND end_date > '2024-07-01';
