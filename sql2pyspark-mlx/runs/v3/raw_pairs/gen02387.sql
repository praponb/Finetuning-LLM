SELECT
    c.id,
    c.name,
    COUNT(o.id) AS order_count,
    ROUND(AVG(o.discount), 2) AS avg_discount,
    ROUND((100 * SUM(CASE WHEN o.channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web
FROM customers c
LEFT OUTER JOIN orders o ON o.customer_id = c.id
WHERE o.amount > 970
GROUP BY c.id, c.name;
