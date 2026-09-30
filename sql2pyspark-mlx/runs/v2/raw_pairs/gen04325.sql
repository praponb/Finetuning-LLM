SELECT status
FROM accounts
WHERE account_type != 'CREDIT' AND customer_id IN (4, 12, 16) AND status = 'OPEN'
