SELECT status FROM accounts WHERE account_type IN ('CREDIT', 'SAVINGS', 'LOAN') INTERSECT SELECT status FROM payments ORDER BY status DESC;
