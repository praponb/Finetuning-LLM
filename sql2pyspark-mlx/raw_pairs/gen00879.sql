SELECT s.sqft, s.city, ss.sale_date
FROM stores s
INNER JOIN store_sales ss ON ss.store_id = s.id;
