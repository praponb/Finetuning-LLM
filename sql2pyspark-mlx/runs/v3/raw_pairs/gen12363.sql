SELECT id, account_type, UPPER(account_type) AS account_type_upper, TRIM(account_type) AS clean_account_type FROM accounts WHERE balance <= 59000 OR status IN ('OPEN', 'CLOSED', 'FROZEN')
