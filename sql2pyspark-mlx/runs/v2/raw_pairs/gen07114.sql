SELECT products.id, products.brand FROM products WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.product_id = products.id AND inventory.quantity IS NULL)
