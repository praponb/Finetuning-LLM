SELECT p.cost, p.brand, ss.store_id, ss.units FROM products p LEFT JOIN store_sales ss ON p.id = ss.product_id WHERE p.cost < 460 AND p.supplier_id = 21
