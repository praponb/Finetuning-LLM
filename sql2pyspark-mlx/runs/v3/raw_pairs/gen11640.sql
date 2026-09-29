SELECT ss.id, ss.sale_date, ss.product_id, s.opened_date, s.city, p.cost FROM store_sales ss LEFT JOIN stores s ON s.id = ss.store_id JOIN products p ON ss.product_id = p.id;
