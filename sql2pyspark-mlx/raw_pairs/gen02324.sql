SELECT
    a.account_type,
    e.txn_type,
    COUNT(*) AS cnt,
    ROUND(SUM(e.amount), 2) AS total_amount,
    GROUPING_ID() AS level_id
FROM transactions e
INNER JOIN accounts a ON e.account_id = a.id
GROUP BY ROLLUP (a.account_type, e.txn_type)
ORDER BY total_amount DESC NULLS LAST;
