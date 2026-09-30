SELECT inventory.store_id, inventory.product_id, products.product_name
FROM inventory
INNER JOIN products ON inventory.product_id = products.id;
