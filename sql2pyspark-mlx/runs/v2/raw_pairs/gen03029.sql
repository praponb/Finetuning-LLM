select city, min(rating) as min_rating, avg(rating) as avg_rating from suppliers where country = 'UK' group by city having min(rating) > 3.8
