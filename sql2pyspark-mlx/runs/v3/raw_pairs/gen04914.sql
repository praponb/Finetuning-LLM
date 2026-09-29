SELECT CASE WHEN amount >= 3500 THEN 'high' WHEN amount >= 2000 THEN 'mid' ELSE 'low' END AS amount_bucket, MAX(txn_time) AS last_txn_time, MAX(amount) - MIN(amount) AS amount_range
FROM transactions
WHERE is_flagged = TRUE AND txn_type IN ('REFUND', 'CREDIT', 'DEBIT')
GROUP BY amount_bucket;
