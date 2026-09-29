SELECT CASE WHEN amount >= 4300 THEN 'high' WHEN amount >= 1400 THEN 'mid' ELSE 'low' END AS amount_bucket, txn_type, SUM(amount) AS total_amount
FROM transactions
WHERE txn_type IN ('DEBIT', 'REFUND', 'CREDIT')
GROUP BY amount_bucket, txn_type
HAVING SUM(amount) > 24000
