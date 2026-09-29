SELECT order_id, status FROM payments WHERE method = 'promptpay' UNION SELECT order_id, status FROM payments WHERE status IN ('REFUNDED', 'FAILED') ORDER BY status;
