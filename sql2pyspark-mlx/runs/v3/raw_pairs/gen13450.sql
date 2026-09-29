SELECT
    id,
    opened_date,
    balance,
    LAG(balance, 1) OVER (ORDER BY opened_date, id) AS prev_balance
FROM accounts
