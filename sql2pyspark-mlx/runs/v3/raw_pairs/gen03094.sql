SELECT sale_date FROM store_sales WHERE store_id IN (2, 3, 4, 6) UNION ALL SELECT opened_date AS sale_date FROM stores;
