SELECT s.id, COUNT(ss.id) store_sale_count, MIN(ss.sale_date) first_sale_date FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id GROUP BY s.id;
