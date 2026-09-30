SELECT city, COUNT(*) AS row_count FROM customers WHERE country = 'SG' OR country IN ('UK', 'SG', 'JP') GROUP BY city;
