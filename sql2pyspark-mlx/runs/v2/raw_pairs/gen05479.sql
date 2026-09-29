SELECT country, city FROM customers WHERE name LIKE '%Grace%' EXCEPT SELECT country, city FROM suppliers;
