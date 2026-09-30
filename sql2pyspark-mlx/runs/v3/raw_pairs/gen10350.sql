SELECT
    o.shipping_fee,
    o.status,
    s.store_name,
    s.sqft
FROM orders o
LEFT OUTER JOIN stores s ON o.store_id = s.id;
