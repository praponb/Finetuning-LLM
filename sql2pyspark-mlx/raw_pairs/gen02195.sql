SELECT balance, status, customer_id
FROM accounts
WHERE status IN ('CLOSED', 'OPEN');
