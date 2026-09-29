SELECT ss.store_id, ss.product_id, s.store_name, s.opened_date FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id;
