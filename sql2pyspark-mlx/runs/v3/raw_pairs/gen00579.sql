SELECT status FROM accounts WHERE customer_id IN (13, 14, 33) EXCEPT SELECT status FROM payments ORDER BY status DESC;
