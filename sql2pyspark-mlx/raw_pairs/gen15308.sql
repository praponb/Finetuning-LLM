SELECT p.brand, p.cost, ss.store_id, ss.product_id, ss.revenue FROM products p JOIN store_sales ss ON p.id = ss.product_id WHERE p.cost < 330
