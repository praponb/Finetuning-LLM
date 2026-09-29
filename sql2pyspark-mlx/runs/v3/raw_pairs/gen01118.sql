select id, city, rating, country from (select *, row_number() over (partition by city order by rating, id) as row_num from suppliers) as ranked where row_num <= 5 order by country desc;
