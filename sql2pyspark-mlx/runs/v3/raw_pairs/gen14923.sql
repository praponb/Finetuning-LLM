SELECT order_id, status, paid_at, UPPER(method) AS method_upper FROM payments WHERE method = 'cash'
