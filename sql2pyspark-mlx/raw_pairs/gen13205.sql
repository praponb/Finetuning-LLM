SELECT signup_date FROM customers WHERE name LIKE 'Al%' UNION SELECT last_restock FROM inventory WHERE product_id IN (1, 2, 6, 9);
