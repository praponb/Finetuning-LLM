SELECT
    i.id,
    i.last_restock,
    i.product_id,
    s.sqft,
    p.supplier_id,
    p.brand
FROM inventory i
RIGHT JOIN stores s ON i.store_id = s.id
JOIN products p ON p.id = i.product_id;
