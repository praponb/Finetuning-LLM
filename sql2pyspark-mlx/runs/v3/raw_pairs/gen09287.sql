SELECT
    i.id,
    i.last_restock,
    s.region,
    s.opened_date,
    p.brand,
    p.supplier_id,
    ARRAY_CONTAINS(p.tags, 'sale') AS is_sale
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON i.product_id = p.id
WHERE p.cost < 320 OR p.category = 'Fashion'
