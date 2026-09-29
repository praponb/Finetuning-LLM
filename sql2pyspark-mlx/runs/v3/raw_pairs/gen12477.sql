SELECT products.id, products.supplier_id FROM products WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id AND store_sales.units = 12) ORDER BY products.id DESC;
