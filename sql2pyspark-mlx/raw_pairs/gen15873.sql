SELECT txn_type, is_flagged, MAX(amount) AS max_amount
FROM transactions
WHERE amount < 1800 OR txn_type IN ('DEBIT', 'CREDIT')
GROUP BY 1, 2
HAVING MAX(amount) < 2000
ORDER BY max_amount;
