select id, country, rating from (select id, country, rating, row_number() over (partition by country order by rating, id) as rnk from suppliers) as r where rnk <= 5 order by country, rating;
