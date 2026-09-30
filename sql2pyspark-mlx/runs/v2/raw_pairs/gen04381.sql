SELECT id, status, order_id, amount, MAX(amount) OVER (PARTITION BY status, order_id) status_max_amount FROM payments;
