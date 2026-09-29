SELECT country, name FROM customers WHERE name IS NOT NULL UNION SELECT country, name FROM customers WHERE LENGTH(name) > 9
