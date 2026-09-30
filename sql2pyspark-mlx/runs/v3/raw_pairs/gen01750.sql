SELECT country FROM suppliers WHERE city = 'Singapore' UNION SELECT country FROM suppliers WHERE rating > 3.3 ORDER BY country;
