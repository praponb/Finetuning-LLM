SELECT
    o.id,
    o.order_date,
    s.opened_date,
    c.city
FROM orders o
FULL OUTER JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE LENGTH(c.name) < 10
