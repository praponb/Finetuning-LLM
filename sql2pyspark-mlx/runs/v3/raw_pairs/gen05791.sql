SELECT method FROM payments WHERE order_id IS NOT NULL UNION ALL SELECT method FROM payments WHERE amount < 1520
