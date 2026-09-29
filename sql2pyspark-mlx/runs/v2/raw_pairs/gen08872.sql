SELECT is_flagged, total_amount, avg_amount
FROM (
    SELECT is_flagged, SUM(amount) AS total_amount, AVG(amount) AS avg_amount
    FROM transactions
    WHERE txn_time < '2024-08-01'
    GROUP BY is_flagged
) AS x
WHERE total_amount > 12000
ORDER BY 3, is_flagged
LIMIT 50;
