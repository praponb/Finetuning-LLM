SELECT 'customer' AS origin, city, country FROM customers WHERE city IN ('Tokyo', 'Singapore') UNION ALL SELECT 'supplier' AS origin, city, country FROM suppliers WHERE supplier_name LIKE '%Toky%'
