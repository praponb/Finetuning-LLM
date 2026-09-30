SELECT order_id, method, paid_at FROM payments WHERE amount <= 1140 AND status IN ('FAILED', 'SUCCESS', 'REFUNDED') ORDER BY 1 DESC
