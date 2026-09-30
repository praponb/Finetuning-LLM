SELECT id, account_type, status
FROM accounts
WHERE customer_id IS NOT NULL AND account_type IN ('CREDIT', 'CHECKING', 'LOAN') AND status IN ('OPEN', 'FROZEN', 'CLOSED');
