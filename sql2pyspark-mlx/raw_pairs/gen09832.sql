SELECT account_type AS label FROM accounts WHERE account_type IS NOT NULL UNION ALL SELECT status AS label FROM payments WHERE status IS NOT NULL;
