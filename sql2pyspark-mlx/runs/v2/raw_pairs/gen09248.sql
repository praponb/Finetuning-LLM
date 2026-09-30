SELECT id, supplier_name, rating, INITCAP(TRIM(city)) AS city_title FROM suppliers WHERE LENGTH(supplier_name) < 7 AND (country = 'JP' OR supplier_name LIKE 'Eu%') ORDER BY id LIMIT 10
