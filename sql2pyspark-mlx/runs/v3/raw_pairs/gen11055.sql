SELECT *, FLOOR(balance) AS balance_whole, ROUND(balance, 2) AS balance_rounded
FROM accounts
WHERE YEAR(opened_date) >= 2024 AND account_type IN ('SAVINGS', 'CREDIT')
