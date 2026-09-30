SELECT status, ROUND(STDDEV(balance), 2) AS stddev_balance, COUNT(balance) AS balance_count
FROM accounts
WHERE balance BETWEEN 33000 AND 40000
GROUP BY status;
