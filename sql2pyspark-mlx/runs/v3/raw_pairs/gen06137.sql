SELECT id, order_id FROM payments WHERE (order_id IN (5, 7) AND status = 'SUCCESS') OR status IN ('FAILED', 'REFUNDED', 'SUCCESS') ORDER BY id DESC LIMIT 10;
