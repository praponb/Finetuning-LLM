SELECT id, rating, supplier_name, CAST(rating AS INT) AS rating_int FROM suppliers WHERE rating > 2.6 AND city = 'Singapore';
