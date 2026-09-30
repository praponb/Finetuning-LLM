SELECT id, supplier_name, CAST(rating AS INT) AS rating_int FROM suppliers WHERE city IS NOT NULL AND LENGTH(supplier_name) > 9;
