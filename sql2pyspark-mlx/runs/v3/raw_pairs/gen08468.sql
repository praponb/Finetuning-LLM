select id, city, rating, lead(rating, 1) over (partition by city order by rating, id) as next_rating from suppliers where rating > 1.8 order by id desc;
