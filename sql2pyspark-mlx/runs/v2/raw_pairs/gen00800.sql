SELECT status, 'order' AS source FROM orders UNION ALL SELECT status, 'payment' AS source FROM payments WHERE amount < 570;
