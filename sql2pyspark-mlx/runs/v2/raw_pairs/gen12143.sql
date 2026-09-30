SELECT rating, city, supplier_name, CAST(rating AS INT) AS rating_int, SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix FROM suppliers;
