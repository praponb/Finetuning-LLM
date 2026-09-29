SELECT order_id FROM payments WHERE TO_DATE(paid_at) < '2024-01-01';
