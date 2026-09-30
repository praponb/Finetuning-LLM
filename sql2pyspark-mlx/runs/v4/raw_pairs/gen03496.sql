SELECT status FROM accounts WHERE customer_id = 26 EXCEPT SELECT status FROM accounts WHERE status <> 'FROZEN' ORDER BY status
