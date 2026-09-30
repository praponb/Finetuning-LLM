SELECT id, paid_at, order_id, amount, ROUND(amount * 1.1, 2) amount_scaled FROM payments WHERE paid_at >= '2023-08-04';
