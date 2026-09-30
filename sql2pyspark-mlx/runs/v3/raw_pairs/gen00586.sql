SELECT
    c.id,
    c.segment,
    COUNT(o.id) order_count,
    ROUND(AVG(o.discount), 2) avg_discount,
    MIN(o.order_date) min_order_date
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.segment
