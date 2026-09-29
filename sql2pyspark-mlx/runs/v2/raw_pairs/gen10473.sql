SELECT id, city, supplier_name, country, UPPER(city) city_upper, UPPER(city) city_upper_2 FROM suppliers WHERE (city != 'London' AND supplier_name LIKE 'E%') OR rating >= 2.2
