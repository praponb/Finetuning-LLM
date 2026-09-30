SELECT
    inventory.id,
    inventory.store_id,
    products.launch_date,
    suppliers.country
FROM inventory
LEFT JOIN products ON inventory.product_id = products.id
LEFT JOIN suppliers ON products.supplier_id = suppliers.id;
