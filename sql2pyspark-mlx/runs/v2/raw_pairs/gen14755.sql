select id, rating, city from suppliers where rating >= (select avg(rating) from suppliers)
