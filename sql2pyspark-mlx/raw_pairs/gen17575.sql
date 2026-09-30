SELECT
    i.last_restock,
    p.category,
    p.price,
    s.sqft,
    s.store_name
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON s.id = i.store_id
WHERE ARRAY_CONTAINS(p.tags, 'eco') AND p.supplier_id IN (18, 27, 34, 40)
