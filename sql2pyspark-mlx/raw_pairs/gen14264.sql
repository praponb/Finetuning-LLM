select id, city, supplier_name, country, concat(city, '/', supplier_name) as city_supplier_name from suppliers where city = 'Tokyo' and city in ('Singapore', 'Bangkok', 'Tokyo')
