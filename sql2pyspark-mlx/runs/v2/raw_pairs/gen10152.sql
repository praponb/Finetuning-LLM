SELECT account_type, status, CONCAT_WS(',', account_type, status) account_type_status_key FROM accounts WHERE balance <= 60000
