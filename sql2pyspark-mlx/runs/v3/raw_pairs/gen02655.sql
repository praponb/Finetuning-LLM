SELECT id, status, amount, order_id, CONCAT(method, ' - ', status) AS method_status FROM payments WHERE order_id IS NULL
