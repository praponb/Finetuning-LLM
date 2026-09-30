select city, min(rating) as min_rating, max(rating) as max_rating from suppliers where city = 'Chiang Mai' group by city;
