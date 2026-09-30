SELECT
    opened_date,
    account_type,
    CAST(balance AS INT) AS balance_int,
    CASE WHEN balance >= 79000 THEN 'High' WHEN balance >= 15000 THEN 'Medium' ELSE 'Low' END AS balance_band
FROM accounts
WHERE customer_id IS NOT NULL
ORDER BY account_type, balance_band DESC
