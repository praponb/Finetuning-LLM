SELECT
    p.order_id,
    o.store_id,
    s.store_name,
    s.city
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
RIGHT JOIN stores s ON o.store_id = s.id
ORDER BY city
