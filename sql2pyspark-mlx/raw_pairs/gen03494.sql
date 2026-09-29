SELECT id, order_id, amount, CONCAT(method, ' - ', status) AS method_status FROM payments;
