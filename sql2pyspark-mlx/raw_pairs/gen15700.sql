SELECT
    channel,
    status,
    COUNT(amount) AS amount_count,
    ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web
FROM orders
GROUP BY channel, status
HAVING AVG(discount) >= 0.1;
