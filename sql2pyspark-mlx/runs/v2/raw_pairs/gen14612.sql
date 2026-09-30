SELECT balance, SUBSTRING(account_type, 1, 2) AS account_type_prefix FROM accounts ORDER BY balance DESC;
