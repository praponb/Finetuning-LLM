SELECT p.region, COUNT(*) AS cnt, ROUND(SUM(src.units), 2) AS total_units FROM store_sales src LEFT JOIN stores p ON src.store_id = p.id GROUP BY CUBE (p.region);
