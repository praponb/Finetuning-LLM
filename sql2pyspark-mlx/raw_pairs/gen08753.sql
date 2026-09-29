SELECT
    s.id,
    s.cost,
    s.delivered_date,
    o.order_date
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
WHERE o.amount >= 620
ORDER BY s.id DESC;
