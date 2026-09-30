SELECT a.id, a.customer_id
FROM accounts a
WHERE a.balance > 73000 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND TO_DATE(t.txn_time) < '2023-04-01')
