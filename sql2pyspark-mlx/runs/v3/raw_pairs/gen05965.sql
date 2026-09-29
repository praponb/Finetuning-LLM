SELECT
    inventory.quantity,
    inventory.last_restock,
    products.supplier_id,
    products.cost,
    suppliers.city,
    suppliers.country
FROM inventory
JOIN products ON inventory.product_id = products.id
JOIN suppliers ON products.supplier_id = suppliers.id
WHERE ARRAY_CONTAINS(products.tags, 'clearance')
ORDER BY city;
