select city, max(rating) max_rating, round(avg(rating), 2) avg_rating from suppliers where supplier_name like '%Sup%' and city = 'Bangkok' group by city order by city;
