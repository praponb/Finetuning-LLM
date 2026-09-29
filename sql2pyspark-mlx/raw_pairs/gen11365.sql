SELECT id, amount, status
FROM orders
WHERE store_id = 31 AND order_date <= '2025-04-21'
ORDER BY id DESC;
