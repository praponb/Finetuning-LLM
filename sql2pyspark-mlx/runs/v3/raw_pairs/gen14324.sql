SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, COUNT(DISTINCT amount) AS unique_accounts
FROM transactions
WHERE merchant IN ('Starbucks', 'Shell', '7-Eleven')
GROUP BY txn_month
HAVING SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) < 29
ORDER BY unique_accounts, txn_month
LIMIT 50;
