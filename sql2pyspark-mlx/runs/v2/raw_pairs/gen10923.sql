SELECT id, country, LENGTH(supplier_name) AS supplier_name_length FROM suppliers WHERE rating < 2.9 ORDER BY id DESC LIMIT 10;
