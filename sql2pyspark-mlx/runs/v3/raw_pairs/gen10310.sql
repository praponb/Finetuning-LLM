select id, country, rating, city, rank() over (partition by country order by rating) rating_rank, row_number() over (partition by city order by rating, id) rn from suppliers order by city;
