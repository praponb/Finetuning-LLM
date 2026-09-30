select country, max(rating) - min(rating) as rating_range, count(*) as row_count from suppliers group by country order by row_count desc, rating_range asc, country asc limit 3;
