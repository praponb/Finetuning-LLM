SELECT id, status, order_id, amount, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount, id) AS rn FROM payments
