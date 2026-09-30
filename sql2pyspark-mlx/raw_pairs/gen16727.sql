SELECT
    o.id,
    o.shipping_fee,
    c.country,
    c.credit_limit,
    s.sqft,
    s.store_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE o.amount >= 1360
ORDER BY id DESC, store_name;
