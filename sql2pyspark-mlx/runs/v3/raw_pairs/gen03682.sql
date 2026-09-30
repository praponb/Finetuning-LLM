SELECT p.stock_qty, p.price, p.product_name, ss.revenue FROM products p LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id ORDER BY stock_qty DESC, revenue DESC
