SELECT id, order_date, store_id FROM orders WHERE status NOT IN ('RETURNED', 'PENDING', 'SHIPPED') INTERSECT SELECT id, order_date, store_id FROM orders WHERE amount > 1020
