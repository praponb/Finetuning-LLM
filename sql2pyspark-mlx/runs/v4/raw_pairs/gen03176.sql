SELECT status, 'account' AS source FROM accounts WHERE balance > 40000 UNION ALL SELECT status, 'payment' AS source FROM payments;
