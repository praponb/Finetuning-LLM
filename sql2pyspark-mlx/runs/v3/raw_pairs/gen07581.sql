SELECT a.id, a.customer_id
FROM accounts a
WHERE a.status IN ('FROZEN', 'CLOSED', 'OPEN') AND a.id IN (SELECT account_id FROM transactions WHERE txn_time >= '2023-11-01')
