SELECT s.id, s.opened_date, s.region, s.city FROM stores s WHERE s.sqft = 16024 AND s.id IN (SELECT store_id FROM inventory WHERE last_restock > '2025-08-01');
