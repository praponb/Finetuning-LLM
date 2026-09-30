SELECT method, status, amount FROM payments WHERE amount > 1380 AND (order_id IN (7, 37) OR status IN ('SUCCESS', 'FAILED'));
