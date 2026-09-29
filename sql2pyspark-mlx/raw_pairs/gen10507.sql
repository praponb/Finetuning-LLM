SELECT id, status, method, paid_at FROM payments WHERE method <> 'cash' AND order_id = 24 AND status = 'SUCCESS'
