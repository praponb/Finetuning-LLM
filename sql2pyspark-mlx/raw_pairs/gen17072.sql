SELECT id, name FROM customers WHERE segment IS NULL AND (LENGTH(name) < 11 OR name NOT LIKE 'Ka%') ORDER BY name, id;
