SELECT 'account' kind, status FROM accounts UNION ALL SELECT 'payment' kind, status FROM payments ORDER BY status;
