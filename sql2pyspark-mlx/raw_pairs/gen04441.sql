SELECT
    is_flagged,
    HOUR(txn_time) AS txn_hour,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount
FROM transactions
WHERE txn_type IN ('DEBIT', 'CREDIT', 'REFUND') AND amount BETWEEN 1100 AND 2100
GROUP BY is_flagged, HOUR(txn_time)
