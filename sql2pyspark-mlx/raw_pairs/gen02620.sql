SELECT
    inventory.id,
    inventory.store_id,
    inventory.last_restock,
    products.supplier_id,
    suppliers.country
FROM inventory
JOIN products ON inventory.product_id = products.id
RIGHT JOIN suppliers ON suppliers.id = products.supplier_id;
