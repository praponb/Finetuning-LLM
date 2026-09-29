SELECT
    account_type,
    balance,
    opened_date,
    ROUND(balance * 1.07, 2) AS adjusted_balance
FROM accounts
WHERE status != 'FROZEN'
ORDER BY adjusted_balance DESC, balance DESC
