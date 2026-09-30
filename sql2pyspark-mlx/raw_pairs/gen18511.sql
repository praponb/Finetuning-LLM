SELECT c.id, c.age, c.is_active, c.name FROM customers c WHERE c.city IN ('London', 'Chiang Mai', 'Tokyo') AND c.id IN (SELECT customer_id FROM orders WHERE store_id = 40) ORDER BY id;
