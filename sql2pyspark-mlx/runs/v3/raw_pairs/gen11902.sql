SELECT
    i.id,
    i.store_id,
    p.price,
    s.country
FROM inventory i
RIGHT JOIN products p ON p.id = i.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand IN ('Acme', 'Globex', 'Umbrella') AND s.city IN ('Chiang Mai', 'Bangkok');
