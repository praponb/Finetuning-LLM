SELECT city, country FROM customers WHERE name IS NULL INTERSECT SELECT city, country FROM suppliers;
