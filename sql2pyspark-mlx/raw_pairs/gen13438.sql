select id, country, city, rating, row_number() over (partition by country, city order by rating, id) as row_num from suppliers
