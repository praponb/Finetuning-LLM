SELECT id, customer_id, store_id, discount, SUBSTRING(status, 1, 2) AS status_prefix FROM orders ORDER BY store_id DESC
