SELECT city, 'supplier' source FROM suppliers WHERE city = 'London' UNION ALL SELECT city, 'store' source FROM stores WHERE store_name LIKE 'Old%';
