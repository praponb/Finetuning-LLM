SELECT id, account_type, UPPER(account_type) AS account_type_upper, CONCAT_WS('-', status, account_type) AS status_account_type_key FROM accounts ORDER BY account_type ASC, account_type_upper ASC;
