SELECT
    s.city,
    s.sqft,
    s.store_name,
    o.customer_id,
    o.channel,
    o.store_id
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id;
