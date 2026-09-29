SELECT store_sales.id, store_sales.units FROM store_sales WHERE store_sales.store_id = 5 AND store_sales.product_id IN (SELECT id FROM products WHERE cost BETWEEN 450 AND 620)
