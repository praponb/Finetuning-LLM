SELECT shipping_fee
FROM orders
WHERE shipping_fee IS NOT NULL
UNION
SELECT monthly_fee
FROM subscriptions
WHERE monthly_fee IS NOT NULL;
