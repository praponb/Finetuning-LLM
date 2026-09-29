SELECT shipping_fee
FROM orders
WHERE shipping_fee BETWEEN 13 AND 27
INTERSECT
SELECT monthly_fee shipping_fee
FROM subscriptions
ORDER BY shipping_fee
