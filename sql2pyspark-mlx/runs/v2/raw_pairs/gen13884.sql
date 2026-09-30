SELECT status, balance, SUBSTRING(account_type, 1, 3) AS account_type_prefix FROM accounts ORDER BY account_type_prefix;
