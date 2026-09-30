SELECT status, SUM(balance) AS total_balance
FROM accounts
GROUP BY 1
HAVING total_balance >= 115000;
