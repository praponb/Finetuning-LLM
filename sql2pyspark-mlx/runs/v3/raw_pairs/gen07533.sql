SELECT
    o.customer_id,
    o.amount,
    o.discount,
    s.shipped_date,
    s.carrier,
    s.cost
FROM orders o
INNER JOIN shipments s ON s.order_id = o.id
WHERE o.amount > 1220
