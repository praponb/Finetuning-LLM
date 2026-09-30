SELECT
    DATE_FORMAT(paid_at, 'yyyy-MM') paid_month,
    method,
    HOUR(paid_at) paid_hour,
    ROUND((100 * SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_card,
    AVG(amount) avg_amount
FROM payments
WHERE TO_DATE(paid_at) < '2023-07-09'
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM'), method, HOUR(paid_at);
