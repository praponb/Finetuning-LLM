SELECT id, status, balance, account_type, FIRST_VALUE(account_type) OVER (PARTITION BY status ORDER BY balance, id) first_account_type FROM accounts ORDER BY status ASC;
