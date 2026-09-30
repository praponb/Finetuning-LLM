select id, country, rating, lag(rating, 1) over (partition by country order by rating, id) prev_rating from suppliers
