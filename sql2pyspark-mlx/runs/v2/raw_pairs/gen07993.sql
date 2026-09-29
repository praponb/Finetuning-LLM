SELECT account_id, COUNT(*) AS cnt
FROM transactions
WHERE merchant IN ('Shell', 'Grab', 'Starbucks') OR txn_time < '2023-07-31'
GROUP BY 1
HAVING COUNT(*) < 3
ORDER BY 2, account_id
LIMIT 10;
