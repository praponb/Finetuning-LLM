SELECT
    i.store_id,
    s.region,
    s.city,
    p.category,
    p.brand
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE s.city IN ('Berlin', 'Bangkok');
