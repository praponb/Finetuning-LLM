SELECT shipping_fee, amount
FROM orders
WHERE customer_id = 36 AND (store_id = 36 OR amount >= 720)
