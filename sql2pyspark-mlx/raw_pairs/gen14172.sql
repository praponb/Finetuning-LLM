SELECT 'account' origin, status FROM accounts UNION ALL SELECT 'payment' origin, status FROM payments WHERE status = 'REFUNDED';
