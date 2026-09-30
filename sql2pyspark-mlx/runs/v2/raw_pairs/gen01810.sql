SELECT status FROM orders WHERE status = 'SHIPPED' EXCEPT SELECT status FROM accounts ORDER BY status;
