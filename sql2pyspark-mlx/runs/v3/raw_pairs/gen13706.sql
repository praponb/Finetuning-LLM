SELECT shipping_fee, 'order' AS source
FROM orders
UNION
SELECT monthly_fee AS shipping_fee, 'subscription' AS source
FROM subscriptions;
