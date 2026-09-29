SELECT a.id, a.balance, a.customer_id
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions WHERE HOUR(txn_time) BETWEEN 9 AND 12)
ORDER BY id ASC
LIMIT 10
