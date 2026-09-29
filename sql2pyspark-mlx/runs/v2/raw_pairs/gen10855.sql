SELECT *
FROM (
    SELECT merchant, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) AS refund_count
    FROM transactions
    WHERE txn_type <> 'FEE'
    GROUP BY merchant, DATE_FORMAT(txn_time, 'yyyy-MM')
) AS sub
WHERE refund_count >= 26;
