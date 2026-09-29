SELECT method, MIN(paid_at) AS min_paid_at FROM payments WHERE amount >= 760 AND status = 'REFUNDED' GROUP BY method
