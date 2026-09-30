SELECT QUARTER(txn_time) AS txn_quarter, MAX(amount) AS max_amount
FROM transactions
WHERE account_id IN (2, 22, 23)
GROUP BY QUARTER(txn_time)
HAVING MAX(amount) >= 2000
ORDER BY txn_quarter DESC
LIMIT 5
