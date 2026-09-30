SELECT id, country, city, rating FROM suppliers WHERE country != 'JP' AND LENGTH(supplier_name) > 7 ORDER BY city;
