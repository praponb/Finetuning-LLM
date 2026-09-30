SELECT status, 'order' AS source FROM orders WHERE amount >= 1520 UNION ALL SELECT status, 'payment' AS source FROM payments WHERE order_id IS NOT NULL;
