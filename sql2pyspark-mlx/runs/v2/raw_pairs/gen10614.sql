SELECT
    id,
    status,
    account_type,
    balance,
    ROUND(balance, 1) AS balance_rounded
FROM accounts
