select id, supplier_name, rating, lower(country) as country_lower from suppliers where country = 'DE' or country = 'JP';
