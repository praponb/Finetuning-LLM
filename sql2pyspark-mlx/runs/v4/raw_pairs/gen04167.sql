SELECT s.city, s.store_name, s.sqft, ss.store_id, ss.sale_date, ss.revenue FROM stores s LEFT OUTER JOIN store_sales ss ON s.id = ss.store_id WHERE ss.product_id = 7 ORDER BY store_id
