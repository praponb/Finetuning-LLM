SELECT id, city, supplier_name, rating, SUBSTRING(city, 1, 4) city_prefix FROM suppliers WHERE supplier_name IS NOT NULL;
