SELECT
    status,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(DISTINCT amount) AS unique_statuss,
    ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
FROM payments
WHERE status IN ('REFUNDED', 'SUCCESS', 'FAILED') AND amount BETWEEN 720 AND 810
GROUP BY status
ORDER BY pct_success
