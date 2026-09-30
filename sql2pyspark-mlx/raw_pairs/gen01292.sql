SELECT 'product' AS source, cost
FROM products
WHERE cost IS NOT NULL
UNION
SELECT 'subscription' AS source, monthly_fee AS cost
FROM subscriptions
WHERE monthly_fee IS NOT NULL
