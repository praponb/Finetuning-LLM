SELECT *, ROUND(balance * 1.07, 2) AS balance_scaled FROM accounts WHERE opened_date BETWEEN '2024-02-18' AND '2024-11-01' ORDER BY account_type DESC;
