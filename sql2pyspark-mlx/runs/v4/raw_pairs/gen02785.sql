SELECT
    i.id,
    i.product_id,
    s.sqft,
    s.store_name,
    p.supplier_id,
    p.product_name
FROM inventory i
LEFT OUTER JOIN stores s ON s.id = i.store_id
LEFT OUTER JOIN products p ON i.product_id = p.id
