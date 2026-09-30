SELECT status, 'account' AS source FROM accounts WHERE balance IS NOT NULL UNION SELECT status, 'payment' AS source FROM payments WHERE method = 'bank_transfer';
