SELECT country, 'customer' AS source FROM customers WHERE city IN ('London', 'Singapore', 'Bangkok') UNION ALL SELECT country, 'supplier' AS source FROM suppliers WHERE rating < 2.2
