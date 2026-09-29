SELECT
    p.id,
    p.order_id,
    o.store_id,
    o.discount,
    c.age
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id;
