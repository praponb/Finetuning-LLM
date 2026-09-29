SELECT p.price, ss.sale_date, ss.product_id FROM products p INNER JOIN store_sales ss ON ss.product_id = p.id;
