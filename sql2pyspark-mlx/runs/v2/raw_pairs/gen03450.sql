SELECT
    channel,
    customer_id,
    ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web,
    COUNT(*) AS total_count
FROM orders
GROUP BY channel, customer_id
