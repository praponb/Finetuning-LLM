select city, count(*) row_count, round(avg(rating), 2) avg_rating from suppliers where country = 'UK' group by city;
