SELECT id, rating FROM suppliers WHERE rating >= (SELECT AVG(rating) FROM suppliers) ORDER BY rating DESC;
