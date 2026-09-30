SELECT MAX(order_date) AS latest_order_date, MAX(discount) - MIN(discount) AS discount_range
FROM orders
WHERE store_id = 17
ORDER BY discount_range
