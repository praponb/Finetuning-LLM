SELECT amount
FROM orders
UNION ALL
SELECT monthly_fee
FROM subscriptions
