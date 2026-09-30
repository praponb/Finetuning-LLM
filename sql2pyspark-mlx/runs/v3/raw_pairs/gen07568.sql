select id, country, rating, city from (select *, row_number() over (partition by country order by rating desc, id) as row_num from suppliers) as r where row_num <= 3
