SELECT p.product_name, p.cost, ss.sale_date, ss.product_id, ss.revenue FROM products p INNER JOIN store_sales ss ON p.id = ss.product_id WHERE ss.revenue <= 2400 OR p.category = 'Home';
