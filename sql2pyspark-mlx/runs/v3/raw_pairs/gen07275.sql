SELECT country, city FROM customers WHERE age <= 40 EXCEPT SELECT country, city FROM suppliers;
