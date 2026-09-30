SELECT
    balance,
    status,
    account_type,
    ROUND(balance * 0.9, 2) AS balance_adjusted
FROM accounts
WHERE opened_date >= '2024-03-21';
