SELECT *, COALESCE(amount, 0) AS amount_filled FROM payments WHERE HOUR(paid_at) BETWEEN 7 AND 10 AND order_id IN (4, 12, 13, 27) AND order_id IS NULL ORDER BY paid_at, id LIMIT 5;
