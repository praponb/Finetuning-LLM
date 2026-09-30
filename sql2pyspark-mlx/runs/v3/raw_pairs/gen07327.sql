SELECT id, customer_id FROM accounts WHERE account_type IN ('LOAN', 'CREDIT', 'CHECKING') UNION ALL SELECT id, customer_id FROM accounts WHERE customer_id IS NOT NULL ORDER BY id
