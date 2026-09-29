SELECT shipping_fee, 'order' origin
FROM orders
WHERE shipping_fee IS NOT NULL
UNION ALL
SELECT monthly_fee, 'subscription' origin
FROM subscriptions
WHERE monthly_fee IS NOT NULL;
