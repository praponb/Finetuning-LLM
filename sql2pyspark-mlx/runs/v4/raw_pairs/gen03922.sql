SELECT id, amount, order_id, status, FLOOR(amount) AS amount_whole FROM payments WHERE order_id IN (7, 13, 14, 21) OR amount BETWEEN 620 AND 1000
