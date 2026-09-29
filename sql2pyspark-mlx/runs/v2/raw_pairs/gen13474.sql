SELECT country, city FROM suppliers WHERE city IN ('Chiang Mai', 'Tokyo') UNION ALL SELECT country, city FROM suppliers WHERE rating BETWEEN 2.3 AND 3.5;
