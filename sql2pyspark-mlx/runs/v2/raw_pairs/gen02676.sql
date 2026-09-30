SELECT order_id FROM payments WHERE amount > 800 EXCEPT SELECT order_id FROM payments WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS');
