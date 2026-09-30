SELECT id, country, city FROM suppliers WHERE supplier_name LIKE 'Li%' UNION ALL SELECT id, country, city FROM suppliers WHERE city = 'New York' ORDER BY city DESC
