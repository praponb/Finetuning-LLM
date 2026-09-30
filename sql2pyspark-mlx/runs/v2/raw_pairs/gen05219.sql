SELECT
    id,
    account_type,
    balance,
    ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS row_num
FROM accounts
