SELECT is_flagged, txn_type, SUM(amount) AS total_amount
FROM transactions
GROUP BY is_flagged, txn_type
HAVING AVG(amount) >= 2000
