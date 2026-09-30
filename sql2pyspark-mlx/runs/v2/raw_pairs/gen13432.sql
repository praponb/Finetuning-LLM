SELECT s.id, s.opened_date, s.sqft, s.city FROM stores s WHERE s.id IN (SELECT store_id FROM inventory WHERE quantity = 102) ORDER BY s.sqft DESC;
