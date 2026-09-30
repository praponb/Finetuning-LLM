SELECT
    id,
    account_type,
    opened_date,
    balance,
    LAG(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance,
    RANK() OVER (PARTITION BY account_type ORDER BY balance) AS rnk
FROM accounts
ORDER BY rnk DESC
