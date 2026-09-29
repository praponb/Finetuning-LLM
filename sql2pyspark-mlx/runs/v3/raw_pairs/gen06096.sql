SELECT
    id,
    balance,
    customer_id,
    status,
    ROUND(balance, 2) AS balance_rounded
FROM accounts
WHERE opened_date <= '2024-01-01';
