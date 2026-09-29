SELECT balance, opened_date, ROUND(balance * 0.85, 2) AS balance_scaled
FROM accounts;
