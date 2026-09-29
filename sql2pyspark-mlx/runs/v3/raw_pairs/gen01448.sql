select city, supplier_name, rating, concat(supplier_name, ', ', city) as supplier_name_city, substring(city, 1, 2) as city_prefix from suppliers where country = 'TH' or supplier_name is not null;
