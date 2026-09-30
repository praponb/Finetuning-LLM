SELECT
    o.discount,
    o.store_id,
    c.name,
    s.opened_date
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON o.store_id = s.id
