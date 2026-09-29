SELECT
    o.id,
    o.amount,
    o.shipping_fee,
    s.sqft,
    s.region,
    c.email
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE LENGTH(c.name) > 11
ORDER BY o.amount;
