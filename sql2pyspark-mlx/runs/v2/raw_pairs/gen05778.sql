SELECT
    merchant,
    SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count,
    COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM transactions
GROUP BY 1;
