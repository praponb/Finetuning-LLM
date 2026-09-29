SELECT p.launch_date, p.stock_qty, p.category, s.rating, INITCAP(TRIM(s.city)) city_title FROM products p JOIN suppliers s ON p.supplier_id = s.id ORDER BY 2 DESC;
