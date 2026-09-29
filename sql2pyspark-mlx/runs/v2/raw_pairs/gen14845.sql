SELECT id, rating FROM suppliers WHERE rating >= (SELECT AVG(rating) FROM suppliers WHERE city NOT IN ('Bangkok', 'Berlin', 'Singapore'));
