SELECT
    inventory.id,
    inventory.quantity,
    products.brand,
    suppliers.city
FROM inventory
FULL OUTER JOIN products ON products.id = inventory.product_id
JOIN suppliers ON products.supplier_id = suppliers.id
WHERE products.brand = 'Initech';
