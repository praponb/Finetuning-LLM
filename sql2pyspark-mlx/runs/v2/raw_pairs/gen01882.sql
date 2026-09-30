SELECT
    account_type,
    ROUND(AVG(balance), 2) AS avg_balance,
    MIN(balance) AS min_balance,
    MAX(opened_date) AS last_opened_date
FROM accounts
GROUP BY account_type
