SELECT s.id, s.opened_date FROM stores s WHERE s.id IN (SELECT store_id FROM orders) ORDER BY s.opened_date, id LIMIT 3;
