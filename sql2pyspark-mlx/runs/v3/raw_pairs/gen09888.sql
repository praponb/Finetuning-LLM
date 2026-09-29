select id, country, rating, city, first_value(city) over (partition by country order by rating desc, id) as first_city from suppliers where supplier_name like '%Lio%'
