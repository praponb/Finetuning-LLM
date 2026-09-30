SELECT id, rating, city, supplier_name FROM suppliers WHERE rating <= (SELECT MIN(rating) FROM suppliers);
