SELECT age FROM customers WHERE segment = 'Consumer' INTERSECT SELECT age FROM customers WHERE is_active ORDER BY age;
