SELECT *
FROM (
    SELECT method, AVG(amount) AS avg_amount, ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 12 AND 20
    GROUP BY method
) AS agg
WHERE avg_amount < 1590
ORDER BY method
LIMIT 1;
