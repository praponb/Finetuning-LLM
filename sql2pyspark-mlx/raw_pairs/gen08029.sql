select id, city, rating, lead(rating, 1) over (partition by city order by rating, id) next_rating from suppliers where supplier_name like '%Glob%'
