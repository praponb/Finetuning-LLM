SELECT city, country FROM customers EXCEPT SELECT city, country FROM suppliers WHERE rating <= 2.4 ORDER BY country;
