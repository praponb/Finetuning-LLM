SELECT id, rating, country FROM suppliers WHERE rating <= (SELECT MIN(rating) FROM suppliers);
