SELECT country, 'customer' record_type FROM customers UNION SELECT country, 'supplier' record_type FROM suppliers WHERE rating < 3.8 ORDER BY country;
