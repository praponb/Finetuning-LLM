SELECT *, CONCAT_WS('-', supplier_name, city) supplier_name_city_key FROM suppliers WHERE country = 'JP' ORDER BY 4;
