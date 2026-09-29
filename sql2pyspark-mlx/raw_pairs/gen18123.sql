SELECT id, order_id, status, COALESCE(method, 'Unknown') method_filled FROM payments WHERE NOT (amount <= 410 AND (order_id = 18 OR TO_DATE(paid_at) < '2024-12-01'));
