SELECT id, rating FROM suppliers WHERE rating > (SELECT MAX(rating) FROM suppliers);
