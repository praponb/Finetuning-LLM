SELECT city, 'customer' AS source FROM customers WHERE name IS NOT NULL UNION SELECT city, 'store' AS source FROM stores WHERE opened_date > '2023-09-01'
