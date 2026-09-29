SELECT
    o.shipping_fee,
    s.sqft,
    c.segment,
    c.email
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.channel IN ('store', 'partner')
ORDER BY sqft DESC, shipping_fee DESC;
