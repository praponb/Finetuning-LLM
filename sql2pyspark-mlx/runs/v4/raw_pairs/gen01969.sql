SELECT city, country, 'customer' AS source FROM customers UNION SELECT city, country, 'supplier' AS source FROM suppliers WHERE supplier_name LIKE 'Si%' ORDER BY source
