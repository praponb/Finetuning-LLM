SELECT p.stock_qty, ss.sale_date, ss.revenue, ss.store_id FROM products p LEFT OUTER JOIN store_sales ss ON ss.product_id = p.id ORDER BY ss.sale_date DESC;
