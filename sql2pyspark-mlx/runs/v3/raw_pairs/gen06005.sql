SELECT ss.id, ss.store_id, s.opened_date, s.city, s.sqft FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id;
