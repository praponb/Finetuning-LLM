select country, city, max(rating) - min(rating) as rating_range from suppliers group by country, city order by city;
