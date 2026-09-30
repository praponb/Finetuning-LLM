SELECT account_type, status, ROUND(AVG(balance), 2) avg_balance, COUNT(DISTINCT balance) unique_customers FROM accounts GROUP BY account_type, status;
