SELECT method, order_id, SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) failed_amount, COUNT(amount) amount_count FROM payments GROUP BY method, order_id;
