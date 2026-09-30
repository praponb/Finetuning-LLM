SELECT id, status FROM orders WHERE order_date > '2023-12-01' EXCEPT SELECT id, status FROM orders WHERE discount < 0.35 ORDER BY status;
