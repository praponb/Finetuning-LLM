SELECT status FROM accounts WHERE status IN ('FROZEN', 'CLOSED', 'OPEN') INTERSECT SELECT status FROM payments WHERE order_id IN (7, 23, 34)
