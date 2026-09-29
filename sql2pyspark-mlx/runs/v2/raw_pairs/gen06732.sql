SELECT account_type, status FROM accounts WHERE customer_id IN (8, 14, 22, 24) UNION SELECT account_type, status FROM accounts WHERE customer_id IN (24, 38, 39) ORDER BY status DESC;
