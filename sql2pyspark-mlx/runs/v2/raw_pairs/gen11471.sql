SELECT
    o.id,
    o.status,
    o.store_id,
    s.city
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE YEAR(o.order_date) = 2025;
