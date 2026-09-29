SELECT o.id, o.store_id FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE TO_DATE(paid_at) >= '2024-08-01');
