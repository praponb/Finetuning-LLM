SELECT ss.id, ss.sale_date, ss.units FROM store_sales ss WHERE ss.sale_date <= '2023-04-01' AND ss.store_id IN (SELECT id FROM stores WHERE LOWER(store_name) LIKE '%ld %') ORDER BY 2;
