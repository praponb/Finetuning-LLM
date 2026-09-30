SELECT store_id, customer_id, status
FROM orders
WHERE amount >= 1310 AND shipping_fee < 26 AND discount >= 0.18
