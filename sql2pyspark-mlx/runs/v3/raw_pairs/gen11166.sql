SELECT city, 'customer' AS source FROM customers UNION SELECT city, 'store' AS source FROM stores WHERE store_name LIKE '%Kios%'
