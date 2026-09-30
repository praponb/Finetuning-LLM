SELECT p.category, p.product_name, ss.sale_date, ss.units, ss.store_id FROM products p LEFT JOIN store_sales ss ON ss.product_id = p.id WHERE ss.revenue IS NOT NULL;
