SELECT p.price, p.stock_qty, p.brand, ss.store_id, ss.revenue FROM products p LEFT JOIN store_sales ss ON ss.product_id = p.id ORDER BY p.stock_qty DESC;
