SELECT s.city, s.region, ss.revenue, ss.store_id, ss.sale_date, ROUND(ss.units, 1) AS units_rounded FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id WHERE s.city <> 'Chiang Mai';
