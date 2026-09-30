SELECT
    p.method,
    p.amount,
    o.order_date,
    o.status,
    s.region,
    s.opened_date,
    o.amount * o.discount amount_times_discount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
RIGHT JOIN stores s ON o.store_id = s.id
ORDER BY s.region
