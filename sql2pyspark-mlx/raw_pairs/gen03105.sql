SELECT id, rating, supplier_name, country FROM suppliers WHERE rating <= (SELECT MIN(rating) FROM suppliers)
