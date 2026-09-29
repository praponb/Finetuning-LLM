SELECT
    s.order_id,
    o.discount,
    c.segment,
    c.city
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
WHERE s.shipped_date BETWEEN '2025-05-23' AND '2025-06-17' AND o.discount >= 0.18;
