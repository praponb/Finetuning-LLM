SELECT amount, 'order' AS record_type
FROM orders
UNION
SELECT monthly_fee, 'subscription' AS record_type
FROM subscriptions;
