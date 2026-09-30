SELECT supplier_name, COALESCE(rating, 0) AS rating_filled, UPPER(supplier_name) AS supplier_name_upper FROM suppliers WHERE supplier_name LIKE '%Gmb%';
