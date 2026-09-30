SELECT id, amount, order_id, status, CASE WHEN method = 'paypal' THEN 1 ELSE 0 END AS is_paypal FROM payments WHERE order_id IS NULL AND order_id = 11;
