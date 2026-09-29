SELECT id, rating, CONCAT(supplier_name, ' ', city) AS supplier_name_city FROM suppliers WHERE rating IS NOT NULL OR supplier_name IS NULL
