SELECT * FROM orders WHERE store_id IN (14, 18, 24, 29) AND status IN ('CANCELLED', 'RETURNED', 'PENDING') ORDER BY amount
