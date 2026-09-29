SELECT status, amount, order_id, COALESCE(amount, 0) AS amount_filled FROM payments WHERE paid_at >= '2024-11-01' AND method = 'card' ORDER BY status, order_id;
