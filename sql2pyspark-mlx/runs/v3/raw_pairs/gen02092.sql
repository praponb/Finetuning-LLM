SELECT
    inventory.id,
    inventory.product_id,
    products.price,
    products.category,
    suppliers.rating,
    suppliers.country
FROM inventory
INNER JOIN products ON products.id = inventory.product_id
INNER JOIN suppliers ON suppliers.id = products.supplier_id
WHERE suppliers.city IN ('Bangkok', 'Tokyo');
