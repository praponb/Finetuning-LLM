SELECT txn_type, is_flagged, MAX(amount) AS max_amount
FROM transactions
WHERE txn_type <> 'REFUND'
GROUP BY txn_type, is_flagged
HAVING COUNT(*) > 2;
