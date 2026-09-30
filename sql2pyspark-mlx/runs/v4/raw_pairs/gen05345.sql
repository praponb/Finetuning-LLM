SELECT t.merchant, a.customer_id
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_time < '2025-01-13';
