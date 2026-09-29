SELECT
    p.status,
    a.txn_type,
    COUNT(*) AS num_rows,
    ROUND(AVG(a.amount), 2) AS avg_amount
FROM transactions a
LEFT JOIN accounts p ON a.account_id = p.id
GROUP BY GROUPING SETS ((p.status, a.txn_type), (p.status), ())
ORDER BY avg_amount DESC NULLS FIRST
