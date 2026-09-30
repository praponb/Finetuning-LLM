SELECT method, order_id FROM payments WHERE method = 'cash' UNION SELECT method, order_id FROM payments WHERE status IN ('SUCCESS', 'FAILED', 'REFUNDED');
