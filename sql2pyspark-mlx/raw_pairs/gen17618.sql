SELECT inventory.id, inventory.last_restock, products.cost FROM inventory LEFT JOIN products ON products.id = inventory.product_id ORDER BY products.cost ASC;
