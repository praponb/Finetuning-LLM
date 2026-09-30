SELECT customer_id, balance, account_type, COALESCE(status, 'unassigned') AS status_filled FROM accounts WHERE customer_id IN (14, 32) AND status = 'CLOSED';
