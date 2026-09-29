SELECT id, supplier_name, country, rating, UPPER(city) city_upper FROM suppliers WHERE country IN ('US', 'UK') AND city = 'Bangkok';
