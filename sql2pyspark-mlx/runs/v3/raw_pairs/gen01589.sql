SELECT id, paid_at, method, order_id, UPPER(status) status_upper FROM payments WHERE paid_at < '2025-05-01' OR method = 'paypal' ORDER BY order_id, id LIMIT 3;
