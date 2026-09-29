select id, city, country, rating, lag(rating, 1) over (partition by city, country order by rating, id) as prev_rating from suppliers where supplier_name is not null
