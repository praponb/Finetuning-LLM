SELECT country, 'customer' AS source FROM customers UNION ALL SELECT country, 'supplier' AS source FROM suppliers;
