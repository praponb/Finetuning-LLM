SELECT amount
FROM orders
UNION
SELECT monthly_fee
FROM subscriptions
ORDER BY amount
