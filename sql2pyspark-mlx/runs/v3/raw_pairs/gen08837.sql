SELECT id, store_id, order_date FROM orders WHERE discount < 0.25 UNION ALL SELECT id, store_id, order_date FROM orders WHERE status IN ('SHIPPED', 'PENDING', 'COMPLETED')
