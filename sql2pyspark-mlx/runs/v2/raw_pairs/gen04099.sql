SELECT id, city, country FROM suppliers WHERE supplier_name LIKE '%Traders%' AND LENGTH(supplier_name) >= 8 ORDER BY id ASC
