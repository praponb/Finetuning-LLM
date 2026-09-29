SELECT MAX(paid_at) AS latest_paid_at FROM payments WHERE (status = 'REFUNDED' AND order_id = 6) OR status IN ('FAILED', 'SUCCESS', 'REFUNDED') ORDER BY latest_paid_at
