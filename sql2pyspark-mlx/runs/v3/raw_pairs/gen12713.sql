SELECT id, rating, supplier_name, country FROM suppliers WHERE rating >= (SELECT MAX(rating) FROM suppliers) ORDER BY supplier_name
