SELECT status, MIN(paid_at) AS min_paid_at, MIN(amount) AS min_amount FROM payments WHERE order_id IN (26, 37) AND (status = 'FAILED' OR paid_at >= '2025-04-01') GROUP BY status;
