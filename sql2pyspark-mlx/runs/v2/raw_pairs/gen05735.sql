SELECT *, CONCAT_WS(',', status, account_type) AS status_account_type_key
FROM accounts
WHERE status = 'CLOSED';
