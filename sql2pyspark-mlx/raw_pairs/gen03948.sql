SELECT products.id, products.launch_date, products.stock_qty FROM products WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id AND store_sales.store_id = 3)
