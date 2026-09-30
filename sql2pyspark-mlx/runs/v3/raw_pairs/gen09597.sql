SELECT id, country, rating, CAST(rating AS INT) AS rating_int FROM suppliers WHERE (supplier_name LIKE '%Parts%' AND rating < 3.1) OR rating <= 2.2;
