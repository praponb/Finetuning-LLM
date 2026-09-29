SELECT s.id, s.city, s.opened_date, s.sqft FROM stores s WHERE s.id IN (SELECT store_id FROM inventory WHERE store_id IN (3, 5, 6)) ORDER BY s.opened_date;
