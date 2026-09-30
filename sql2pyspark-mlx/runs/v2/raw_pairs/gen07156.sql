SELECT products.id, products.supplier_id FROM products WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.product_id = products.id AND YEAR(inventory.last_restock) >= 2024);
