SELECT id, status FROM orders WHERE channel IN ('partner', 'web', 'store') UNION ALL SELECT id, status FROM orders WHERE MONTH(order_date) IN (4, 11, 12) ORDER BY id
