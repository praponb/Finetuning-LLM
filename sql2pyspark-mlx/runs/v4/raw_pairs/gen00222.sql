SELECT * FROM (SELECT city, is_active, COUNT(*) AS customer_count FROM customers WHERE is_active = TRUE GROUP BY city, is_active) AS sub WHERE customer_count < 5 ORDER BY city ASC;
