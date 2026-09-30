select id, country, supplier_name, upper(country) as country_upper from suppliers where city = 'New York' order by country_upper, id limit 5;
