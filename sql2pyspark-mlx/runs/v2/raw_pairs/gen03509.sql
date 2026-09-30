SELECT
    o.id,
    o.shipping_fee,
    o.customer_id,
    s.city,
    s.region,
    c.age
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
JOIN customers c ON c.id = o.customer_id;
