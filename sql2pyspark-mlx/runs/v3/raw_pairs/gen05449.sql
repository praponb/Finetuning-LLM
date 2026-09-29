SELECT country, city, 'customer' AS source FROM customers UNION SELECT country, city, 'supplier' AS source FROM suppliers WHERE city IN ('Chiang Mai', 'New York', 'London');
