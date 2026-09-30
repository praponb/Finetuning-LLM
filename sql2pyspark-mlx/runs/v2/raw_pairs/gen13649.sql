SELECT id, balance, account_type, CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit FROM accounts;
