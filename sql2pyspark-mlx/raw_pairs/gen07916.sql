SELECT
    o.id,
    o.channel,
    o.store_id,
    t.pct_refunded
FROM orders o
LEFT JOIN (SELECT order_id, ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id;
