SELECT country, name FROM customers WHERE city IS NULL INTERSECT SELECT country, name FROM customers WHERE age >= 35
