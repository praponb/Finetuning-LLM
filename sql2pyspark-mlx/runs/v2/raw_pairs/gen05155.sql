SELECT ss.product_id, s.opened_date FROM store_sales ss JOIN stores s ON ss.store_id = s.id WHERE s.region = 'Central';
