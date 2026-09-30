SELECT
    status,
    ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash,
    AVG(amount) AS avg_amount,
    COUNT(amount) AS amount_count
FROM payments
WHERE method = 'card' OR status IN ('FAILED', 'SUCCESS', 'REFUNDED')
GROUP BY status;
