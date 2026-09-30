SELECT country FROM customers UNION ALL SELECT country FROM suppliers WHERE city = 'Berlin' ORDER BY country DESC;
