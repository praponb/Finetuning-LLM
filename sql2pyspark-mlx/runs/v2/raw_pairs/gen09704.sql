SELECT is_flagged, txn_type, AVG(amount) AS avg_amount
FROM transactions
GROUP BY is_flagged, txn_type;
