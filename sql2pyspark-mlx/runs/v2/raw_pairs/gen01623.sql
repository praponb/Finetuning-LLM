SELECT ss.store_id, ss.sale_date, s.region
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY ss.sale_date;
