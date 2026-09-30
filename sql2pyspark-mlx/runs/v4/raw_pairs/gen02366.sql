SELECT c.id, c.name, c.age FROM customers c WHERE c.id IN (SELECT customer_id FROM sessions WHERE browser = 'Firefox') ORDER BY age;
