SELECT supplier_name, rating, city, REPLACE(supplier_name, ' ', '-') supplier_name_slug FROM suppliers WHERE city = 'Tokyo';
