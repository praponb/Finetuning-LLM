SELECT signup_date FROM customers UNION SELECT last_restock FROM inventory WHERE quantity IS NOT NULL ORDER BY signup_date;
