SELECT
    o.status,
    o.customer_id,
    MAX(p.amount) - MIN(p.amount) amount_range,
    ROUND(AVG(p.amount), 2) avg_amount
FROM payments p
RIGHT OUTER JOIN orders o ON o.id = p.order_id
GROUP BY o.status, o.customer_id
ORDER BY avg_amount, status, customer_id
LIMIT 10;
