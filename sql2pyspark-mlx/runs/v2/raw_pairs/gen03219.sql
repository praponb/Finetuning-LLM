SELECT s.cost, o.order_date, o.customer_id
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE o.status IN ('CANCELLED', 'RETURNED', 'COMPLETED') AND o.discount BETWEEN 0.33 AND 0.39
ORDER BY o.customer_id DESC;
