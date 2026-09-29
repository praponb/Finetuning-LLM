SELECT
    s.id,
    s.shipped_date,
    s.cost,
    o.customer_id,
    o.channel,
    c.email
FROM shipments s
FULL OUTER JOIN orders o ON o.id = s.order_id
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.amount >= 650
ORDER BY customer_id DESC;
