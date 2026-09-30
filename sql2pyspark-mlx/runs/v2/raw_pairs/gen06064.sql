SELECT
    customer_id,
    balance,
    CONCAT_WS(',', status, account_type) AS status_account_type_key,
    FLOOR(balance) AS balance_whole
FROM accounts
WHERE account_type = 'LOAN';
