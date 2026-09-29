SELECT monthly_fee
FROM subscriptions
EXCEPT
SELECT shipping_fee AS monthly_fee
FROM orders
WHERE store_id IS NULL
ORDER BY monthly_fee;
