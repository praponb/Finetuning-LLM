SELECT
    account_type,
    opened_date,
    ABS(balance) AS abs_balance,
    ROUND(balance, 2) AS balance_rounded
FROM accounts
WHERE status = 'FROZEN'
