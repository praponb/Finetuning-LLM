SELECT status, 'account' source FROM accounts WHERE status IN ('FROZEN', 'OPEN') UNION ALL SELECT status, 'payment' source FROM payments WHERE method IN ('card', 'bank_transfer') ORDER BY source;
