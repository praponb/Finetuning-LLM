SELECT id, balance, RANK() OVER (ORDER BY balance) AS rnk
FROM accounts
ORDER BY rnk DESC;
