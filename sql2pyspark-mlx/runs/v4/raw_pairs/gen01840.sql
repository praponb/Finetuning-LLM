SELECT ss.revenue, s.city FROM store_sales ss INNER JOIN stores s ON s.id = ss.store_id;
