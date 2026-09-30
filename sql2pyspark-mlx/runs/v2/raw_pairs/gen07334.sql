SELECT s.id, s.region, s.city FROM stores s WHERE s.id IN (SELECT store_id FROM store_sales WHERE revenue <= 2000);
