SELECT
    o.order_date,
    o.shipping_fee,
    c.is_active,
    c.email,
    s.region,
    s.opened_date
FROM orders o
LEFT OUTER JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON s.id = o.store_id
WHERE s.store_name LIKE '%Riversi%' OR c.age <> 55
ORDER BY shipping_fee DESC;
