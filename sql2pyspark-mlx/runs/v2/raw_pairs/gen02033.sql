SELECT products.supplier_id, products.product_name, products.stock_qty, inventory.product_id FROM products LEFT JOIN inventory ON products.id = inventory.product_id ORDER BY inventory.product_id
