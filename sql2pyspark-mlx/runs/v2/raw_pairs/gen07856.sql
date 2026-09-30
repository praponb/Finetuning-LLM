SELECT
    p.amount,
    o.order_date,
    s.opened_date,
    s.city
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
INNER JOIN stores s ON o.store_id = s.id
ORDER BY order_date ASC;
