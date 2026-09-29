SELECT id, is_active, city, segment FROM customers WHERE LOWER(email) LIKE '%aro%' ORDER BY id ASC LIMIT 20;
