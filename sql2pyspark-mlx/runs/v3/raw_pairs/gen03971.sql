select supplier_name, rating, city, trim(supplier_name) clean_supplier_name from suppliers where (city is null and rating <= 2.6) or length(supplier_name) < 8 order by rating;
