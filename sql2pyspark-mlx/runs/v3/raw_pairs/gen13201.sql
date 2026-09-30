SELECT id, store_id, amount FROM orders WHERE order_date < '2023-08-23' AND order_date BETWEEN '2023-05-12' AND '2025-06-12' ORDER BY id LIMIT 10
