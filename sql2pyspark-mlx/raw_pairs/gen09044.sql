SELECT *
FROM (
    SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, COUNT(*) AS total_count
    FROM transactions
    GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM')
) AS x
WHERE total_count < 6
ORDER BY txn_month DESC;
