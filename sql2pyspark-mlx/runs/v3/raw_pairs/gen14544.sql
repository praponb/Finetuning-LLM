SELECT monthly_fee AS value
FROM subscriptions
WHERE customer_id IS NULL
UNION ALL
SELECT cost AS value
FROM products
