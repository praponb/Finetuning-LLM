SELECT id, rating, country, city FROM suppliers WHERE rating >= (SELECT AVG(rating) FROM suppliers WHERE city IN ('Bangkok', 'London'));
