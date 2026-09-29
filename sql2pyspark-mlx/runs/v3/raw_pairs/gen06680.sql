select id, rating, country from suppliers where rating >= (select max(rating) from suppliers)
