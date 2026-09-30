SELECT id, account_type, customer_id, status FROM accounts WHERE status = 'FROZEN' AND customer_id IN (10, 24);
