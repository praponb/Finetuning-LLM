SELECT
    o.id,
    o.customer_id,
    s.store_name,
    s.sqft
FROM orders o
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE o.discount >= 0.27;
