select id, country, rating, supplier_name, concat(city, '/', supplier_name) as city_supplier_name from suppliers where country in ('UK', 'DE') and (supplier_name is not null or rating <= 3.9)
