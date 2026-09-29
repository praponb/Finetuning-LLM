SELECT account_type, COUNT(DISTINCT balance) AS unique_account_types, ROUND(AVG(balance), 2) AS avg_balance, MAX(opened_date) AS last_opened_date FROM accounts GROUP BY 1
