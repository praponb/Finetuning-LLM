SELECT id, rating, supplier_name FROM suppliers WHERE rating > (SELECT MAX(rating) FROM suppliers) ORDER BY rating DESC
