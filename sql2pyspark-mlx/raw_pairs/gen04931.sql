SELECT
    o.discount,
    s.store_name,
    s.region,
    c.email
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
WHERE s.sqft = 7255 OR o.order_date >= '2023-05-27'
ORDER BY email;
