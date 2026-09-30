SELECT status, 'order' AS origin FROM orders UNION ALL SELECT status, 'account' AS origin FROM accounts WHERE account_type != 'SAVINGS';
