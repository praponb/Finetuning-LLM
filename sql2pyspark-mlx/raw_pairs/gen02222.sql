SELECT 'product' AS origin, cost
FROM products
WHERE cost IS NOT NULL
UNION ALL
SELECT 'subscription' AS origin, monthly_fee
FROM subscriptions
WHERE monthly_fee IS NOT NULL
ORDER BY cost;
